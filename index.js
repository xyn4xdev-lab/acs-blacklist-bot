const {
    Client,
    GatewayIntentBits,
    SlashCommandBuilder,
    REST,
    Routes,
    AuditLogEvent,
    EmbedBuilder,
    AttachmentBuilder,
    ActionRowBuilder,
    ButtonBuilder,
    ButtonStyle,
    ComponentType
} = require("discord.js");
const sqlite3 = require("sqlite3").verbose();
const config = require("./config.json");
const fs = require('fs').promises;
const path = require('path');

// ================= BOT EMOJI =================
const BOT_EMOJI = "<:emoji-name:emoji-id>";

// ================= RATE LIMITER =================
class RateLimiter {
    constructor(delay = 1000) {
        this.delay = delay;
        this.lastRequest = 0;
        this.queue = [];
        this.processing = false;
    }
    
    async execute(fn) {
        return new Promise((resolve, reject) => {
            this.queue.push({ fn, resolve, reject });
            this.processQueue();
        });
    }
    
    async processQueue() {
        if (this.processing || this.queue.length === 0) return;
        
        this.processing = true;
        const now = Date.now();
        const timeToWait = Math.max(0, this.delay - (now - this.lastRequest));
        
        await new Promise(r => setTimeout(r, timeToWait));
        
        const item = this.queue.shift();
        this.lastRequest = Date.now();
        
        try {
            const result = await item.fn();
            item.resolve(result);
        } catch (error) {
            item.reject(error);
        }
        
        this.processing = false;
        setTimeout(() => this.processQueue(), 0);
    }
}

// ================= CLIENT =================
const client = new Client({
    intents: [
        GatewayIntentBits.Guilds,
        GatewayIntentBits.GuildMembers,
        GatewayIntentBits.GuildBans,
        GatewayIntentBits.GuildModeration,
        GatewayIntentBits.MessageContent
    ]
});

// ================= DATABASE =================
const db = new sqlite3.Database("./blacklist.db");
db.serialize(() => {
    db.run(`CREATE TABLE IF NOT EXISTS blacklist (
        userId TEXT PRIMARY KEY,
        reason TEXT,
        addedAt INTEGER,
        addedBy TEXT
    )`);
    
    // Create server_blacklist table for banned servers
    db.run(`CREATE TABLE IF NOT EXISTS server_blacklist (
        guildId TEXT PRIMARY KEY,
        reason TEXT,
        addedAt INTEGER,
        addedBy TEXT,
        notified BOOLEAN DEFAULT 0
    )`);
    
    // Create strikes table with count column
    db.run(`CREATE TABLE IF NOT EXISTS strikes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        guildId TEXT,
        guildName TEXT,
        userId TEXT,
        executor TEXT,
        executorId TEXT,
        executorType TEXT DEFAULT 'user',
        timestamp INTEGER,
        count INTEGER DEFAULT 1,
        UNIQUE(guildId, userId)
    )`);
    
    // Ensure all columns exist (for existing tables)
    db.all(`PRAGMA table_info(strikes)`, (err, columns) => {
        if (!err && columns) {
            const hasCount = columns.some(col => col.name === 'count');
            if (!hasCount) {
                db.run(`ALTER TABLE strikes ADD COLUMN count INTEGER DEFAULT 1`);
                console.log("✅ Added count column to strikes table");
            }
            
            const hasGuildName = columns.some(col => col.name === 'guildName');
            if (!hasGuildName) {
                db.run(`ALTER TABLE strikes ADD COLUMN guildName TEXT`);
                console.log("✅ Added guildName column to strikes table");
            }
            
            const hasExecutorId = columns.some(col => col.name === 'executorId');
            if (!hasExecutorId) {
                db.run(`ALTER TABLE strikes ADD COLUMN executorId TEXT`);
                console.log("✅ Added executorId column to strikes table");
            }
            
            const hasExecutorType = columns.some(col => col.name === 'executorType');
            if (!hasExecutorType) {
                db.run(`ALTER TABLE strikes ADD COLUMN executorType TEXT DEFAULT 'user'`);
                console.log("✅ Added executorType column to strikes table");
            }
        }
    });
    
    // Check if notified column exists in server_blacklist
    db.all(`PRAGMA table_info(server_blacklist)`, (err, columns) => {
        if (!err && columns) {
            const hasNotified = columns.some(col => col.name === 'notified');
            if (!hasNotified) {
                db.run(`ALTER TABLE server_blacklist ADD COLUMN notified BOOLEAN DEFAULT 0`);
                console.log("✅ Added notified column to server_blacklist table");
            }
        }
    });
    
    db.run(`CREATE TABLE IF NOT EXISTS strike_logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        guildId TEXT,
        guildName TEXT,
        strikeId INTEGER,
        action TEXT,
        details TEXT,
        timestamp INTEGER
    )`);
    
    db.run(`CREATE TABLE IF NOT EXISTS owner_dm_status (
        guildId TEXT PRIMARY KEY,
        dmAttempted BOOLEAN DEFAULT 0,
        lastAttempt INTEGER
    )`);
    
    db.run(`CREATE TABLE IF NOT EXISTS modlogs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        userId TEXT,
        action TEXT,
        details TEXT,
        timestamp INTEGER
    )`);
    
    db.run(`CREATE TABLE IF NOT EXISTS daily_stats (
        date TEXT PRIMARY KEY,
        bans INTEGER DEFAULT 0,
        unbans INTEGER DEFAULT 0,
        strikes INTEGER DEFAULT 0
    )`);
    
    db.run(`CREATE TABLE IF NOT EXISTS performance_stats (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        action TEXT,
        duration INTEGER,
        timestamp INTEGER
    )`);
});

// ================= PERFORMANCE TRACKING =================
const performance = {
    bansExecuted: 0,
    unbansExecuted: 0,
    strikesIssued: 0,
    startTime: Date.now(),
    errors: 0,
    successes: 0
};

function trackPerformance(action, duration) {
    db.run(`INSERT INTO performance_stats (action, duration, timestamp) VALUES (?, ?, ?)`,
           [action, duration, Date.now()]);
}

// ================= RATE LIMITERS =================
const banRateLimiter = new RateLimiter(750);
const unbanRateLimiter = new RateLimiter(750);

// ================= COMMANDS =================
const commands = [
    new SlashCommandBuilder()
    .setName("blacklist")
    .setDescription("Blacklist commands")
    .addSubcommand(s =>
        s.setName("add")
        .setDescription("Blacklist a user globally")
        .addStringOption(o => o.setName("userid").setDescription("User ID").setRequired(true))
        .addStringOption(o => o.setName("reason").setDescription("Reason").setRequired(true))
    )
    .addSubcommand(s =>
        s.setName("server")
        .setDescription("Blacklist a server (bot will go completely silent)")
        .addStringOption(o => o.setName("guildid").setDescription("Server/Guild ID").setRequired(true))
        .addStringOption(o => o.setName("reason").setDescription("Reason").setRequired(true))
    )
    .addSubcommand(s =>
        s.setName("serverlist")
        .setDescription("Show all blacklisted servers")
        .addIntegerOption(o => o.setName("page").setDescription("Page number").setRequired(false).setMinValue(1))
    )
    .addSubcommand(s =>
        s.setName("list")
        .setDescription("Show all blacklisted users")
        .addIntegerOption(o => o.setName("page").setDescription("Page number").setRequired(false).setMinValue(1))
    ),
    new SlashCommandBuilder()
    .setName("unblacklist")
    .setDescription("Unblacklist commands")
    .addSubcommand(s =>
        s.setName("user")
        .setDescription("Unblacklist a user globally")
        .addStringOption(o => o.setName("userid").setDescription("User ID").setRequired(true))
        .addStringOption(o => o.setName("reason").setDescription("Reason").setRequired(true))
    )
    .addSubcommand(s =>
        s.setName("server")
        .setDescription("Remove a server from blacklist")
        .addStringOption(o => o.setName("guildid").setDescription("Server/Guild ID").setRequired(true))
        .addStringOption(o => o.setName("reason").setDescription("Reason").setRequired(true))
    ),
    new SlashCommandBuilder()
    .setName("search")
    .setDescription("Search for a specific user in the blacklist")
    .addStringOption(o => o.setName("userid").setDescription("User ID to search for").setRequired(true)),
    new SlashCommandBuilder()
    .setName("modlogs")
    .setDescription("View logs for a user")
    .addStringOption(o => o.setName("userid").setDescription("User ID").setRequired(true)),
    new SlashCommandBuilder()
    .setName("strikes")
    .setDescription("View strike information for a guild")
    .addStringOption(o => o.setName("guildid").setDescription("Guild ID (optional)").setRequired(false)),
    new SlashCommandBuilder()
    .setName("reload")
    .setDescription("Re-enforce blacklist"),
    new SlashCommandBuilder()
    .setName("performance")
    .setDescription("View bot performance metrics"),
    new SlashCommandBuilder()
    .setName("export")
    .setDescription("Export blacklist data")
    .addStringOption(o => 
        o.setName("format")
        .setDescription("Export format")
        .addChoices(
            { name: "JSON", value: "json" },
            { name: "CSV", value: "csv" }
        )
        .setRequired(true)
    ),
    new SlashCommandBuilder()
    .setName("import")
    .setDescription("Import blacklist data")
    .addAttachmentOption(o => 
        o.setName("file")
        .setDescription("Data file to import (JSON or CSV)")
        .setRequired(true)
    )
];

// Register commands only in control guild
const rest = new REST({ version: "10" }).setToken(config.token);
(async () => {
    try {
        await rest.put(
            Routes.applicationGuildCommands(config.clientId, config.controlGuildId),
            { body: commands }
        );
        console.log("Slash commands registered successfully");
    } catch (error) {
        console.error("Failed to register commands:", error);
    }
})();

// ================= UTILS =================
function isAuthorized(member) {
    if (config.ownerIds?.includes(member.id)) return true;
    return member.roles.cache.some(r => config.allowedRoles.includes(r.id));
}

function logAction(userId, action, details = "") {
    return new Promise((resolve, reject) => {
        db.run(
            `INSERT INTO modlogs (userId, action, details, timestamp) VALUES (?, ?, ?, ?)`,
            [userId, action, details, Date.now()],
            function(err) {
                if (err) reject(err);
                else resolve(this.lastID);
            }
        );
    });
}

async function logGlobal(embed) {
    try {
        const g = await client.guilds.fetch(config.logGuildId);
        const c = await g.channels.fetch(config.logChannelId);
        await c.send({ embeds: [embed] });
    } catch (error) {
        console.error("Failed to log globally:", error);
    }
}

function formatDuration(ms) {
    const seconds = Math.floor(ms / 1000);
    const minutes = Math.floor(seconds / 60);
    const hours = Math.floor(minutes / 60);
    const days = Math.floor(hours / 24);
    
    if (days > 0) return `${days}d ${hours % 24}h`;
    if (hours > 0) return `${hours}h ${minutes % 60}m`;
    if (minutes > 0) return `${minutes}m ${seconds % 60}s`;
    return `${seconds}s`;
}

function updateDailyStats(type) {
    const today = new Date().toISOString().split('T')[0];
    db.run(`INSERT OR IGNORE INTO daily_stats (date) VALUES (?)`, [today]);
    db.run(`UPDATE daily_stats SET ${type} = ${type} + 1 WHERE date = ?`, [today]);
}

// ================= CHECK IF SERVER IS BLACKLISTED =================
async function isServerBlacklisted(guildId) {
    return new Promise((resolve) => {
        db.get(`SELECT * FROM server_blacklist WHERE guildId = ?`, [guildId], (err, row) => {
            if (err || !row) {
                resolve(false);
            } else {
                resolve(true);
            }
        });
    });
}

async function getServerBlacklistReason(guildId) {
    return new Promise((resolve) => {
        db.get(`SELECT reason FROM server_blacklist WHERE guildId = ?`, [guildId], (err, row) => {
            if (err || !row) {
                resolve(null);
            } else {
                resolve(row.reason);
            }
        });
    });
}

// ================= DM USER FUNCTION =================
async function sendBlacklistDM(userId, reason, bannedServersCount, moderatorId) {
    try {
        const user = await client.users.fetch(userId);
        
        // Create appeal link from config or use default
        const appealLink = config.appealServerLink || "https://discord.gg/your-appeal-server";
        
        const dmEmbed = new EmbedBuilder()
        .setTitle(`${BOT_EMOJI} ACS Global Blacklist Notification`)
        .setColor(0xFF0000)
        .setDescription(`You have been globally blacklisted from ACS.`)
        .addFields([
            { name: "Reason", value: reason || "No reason provided", inline: false },
            { name: "Bans Applied", value: `${bannedServersCount} server(s)`, inline: true },
            { name: "Appeal", value: `If you believe this was a mistake, you can appeal at:\n${appealLink}`, inline: false }
        ])
        .setFooter({ text: "ACS Blacklist System" })
        .setTimestamp();
        
        await user.send({ embeds: [dmEmbed] });
        console.log(`DM sent to ${user.tag} (${userId}) about blacklist`);
        return true;
    } catch (error) {
        console.log(`Could not send DM to ${userId}:`, error.message);
        return false;
    }
}

// ================= FIND FIRST TEXT CHANNEL =================
async function findFirstTextChannel(guild) {
    try {
        // Try to find the first text channel the bot can send messages in
        const channels = guild.channels.cache
        .filter(c => c.type === 0 && c.permissionsFor(guild.members.me).has('SendMessages'))
        .sort((a, b) => a.position - b.position);
        
        return channels.first();
    } catch (error) {
        console.error(`Error finding channel in ${guild.name}:`, error);
        return null;
    }
}

// ================= CHECK IF OWNER DM WAS ALREADY ATTEMPTED =================
async function hasOwnerDMBeenAttempted(guildId) {
    return new Promise((resolve) => {
        db.get(`SELECT dmAttempted FROM owner_dm_status WHERE guildId = ?`, [guildId], (err, row) => {
            if (err || !row) {
                resolve(false);
            } else {
                resolve(row.dmAttempted === 1);
            }
        });
    });
}

async function markOwnerDMAsAttempted(guildId) {
    db.run(`INSERT OR REPLACE INTO owner_dm_status (guildId, dmAttempted, lastAttempt) VALUES (?, 1, ?)`,
           [guildId, Date.now()]);
}

// ================= STRIKE NOTIFICATION FUNCTION =================
async function sendStrikeNotifications(guildId, userId, executor, executorId, executorType, strikeNumber, blacklistReason) {
    try {
        const guild = client.guilds.cache.get(guildId);
        if (!guild) {
            console.log(`Guild ${guildId} not found for strike notification`);
            return;
        }
        
        // Check if server is blacklisted - if so, do absolutely NOTHING
        const isBlacklisted = await isServerBlacklisted(guildId);
        if (isBlacklisted) {
            console.log(`Guild ${guild.name} is blacklisted - skipping ALL notifications (COMPLETE RADIO SILENCE)`);
            return;
        }
        
        const user = await client.users.fetch(userId).catch(() => ({ tag: "Unknown User", id: userId }));
        
        // Get the unbanner user object properly
        let unbannerUser = null;
        let unbannerDisplay = "Unknown";
        
        if (executorId && executorId !== "Unknown" && /^\d{17,19}$/.test(executorId)) {
            try {
                unbannerUser = await client.users.fetch(executorId);
                unbannerDisplay = unbannerUser.tag;
            } catch (fetchError) {
                console.log(`Could not fetch user with ID ${executorId}:`, fetchError.message);
                unbannerDisplay = executor;
            }
        } else {
            unbannerDisplay = executor;
        }
        
        // Add executor type indicator
        const executorDisplay = executorType === 'bot' ? `${unbannerDisplay} (via bot command)` : unbannerDisplay;
        
        console.log(`Sending strike ${strikeNumber} notifications for guild ${guild.name}`);
        
        // Create the notification embed
        const notificationEmbed = new EmbedBuilder()
        .setTitle(`${BOT_EMOJI} STRIKE ${strikeNumber} - Unban Attempt Detected`)
        .setColor(strikeNumber === 1 ? 0xFFA500 : strikeNumber === 2 ? 0xFF6600 : 0xFF0000)
        .setDescription(`A blacklisted user was unbanned in **${guild.name}**`)
        .addFields([
            { name: "👤 Blacklisted User", value: `${user.tag} (${userId})`, inline: false },
            { name: "🛡️ Unbanned By", value: executorDisplay, inline: true },
            { name: "📋 Blacklist Reason", value: blacklistReason || "No reason provided", inline: true },
            { name: "⚠️ Strike Count", value: `${strikeNumber}`, inline: true },
            { name: "🏠 Server", value: guild.name, inline: true },
            { name: "⏰ Time", value: `<t:${Math.floor(Date.now()/1000)}:R>`, inline: true }
        ])
        .setFooter({ text: `Strike ${strikeNumber}` })
        .setTimestamp();
        
        // 1. DM THE UNBANNER (every time they unban a blacklisted user) - Only if it's a real user, not a bot
        if (unbannerUser && unbannerUser.id && executorType === 'user') {
            try {
                const unbannerDM = new EmbedBuilder()
                .setTitle(`${BOT_EMOJI} Warning: You Unbanned a Blacklisted User`)
                .setColor(strikeNumber === 1 ? 0xFFA500 : strikeNumber === 2 ? 0xFF6600 : 0xFF0000)
                .setDescription(`You unbanned a user who is globally blacklisted in the ACS network.`)
                .addFields([
                    { name: "👤 User Unbanned", value: `${user.tag} (${userId})`, inline: false },
                    { name: "📋 Blacklist Reason", value: blacklistReason || "No reason provided", inline: true },
                    { name: "🏠 Server", value: guild.name, inline: true },
                    { name: "⚠️ Strike Status", value: `Strike ${strikeNumber} for this server`, inline: true }
                ])
                .setFooter({ text: "ACS Blacklist System" })
                .setTimestamp();
                
                await unbannerUser.send({ embeds: [unbannerDM] });
                console.log(`DM sent to unbanner ${unbannerUser.tag}`);
            } catch (dmError) {
                console.log(`Could not DM unbanner ${unbannerUser.tag}:`, dmError.message);
            }
        } else {
            console.log(`Cannot send DM to unbanner: ${executorType} (${executor})`);
        }
        
        // 2. NOTIFY SERVER OWNER (on all strikes)
        try {
            const owner = await guild.fetchOwner();
            
            let ownerMessage = "";
            if (strikeNumber === 1) {
                ownerMessage = `⚠️ **NOTICE:** Your server **${guild.name}** has received its **FIRST STRIKE** for unbanning a blacklisted user.`;
            } else if (strikeNumber === 2) {
                ownerMessage = `⚠️ **WARNING:** Your server **${guild.name}** has received its **SECOND STRIKE** for unbanning blacklisted users.`;
            } else {
                ownerMessage = `⚠️ **NOTICE:** Your server **${guild.name}** has received strike **#${strikeNumber}** for unbanning blacklisted users.`;
            }
            
            const ownerDM = new EmbedBuilder()
            .setTitle(strikeNumber === 1 ? "⚠️ Server Notice: First Strike" : strikeNumber === 2 ? "⚠️ Server Warning: Second Strike" : `⚠️ Strike #${strikeNumber} Notification`)
            .setColor(strikeNumber === 1 ? 0xFFA500 : strikeNumber === 2 ? 0xFF6600 : 0xFF0000)
            .setDescription(ownerMessage)
            .addFields([
                { name: "👤 User Unbanned", value: `${user.tag} (${userId})`, inline: true },
                { name: "🛡️ Unbanned By", value: executorDisplay, inline: true },
                { name: "📋 Blacklist Reason", value: blacklistReason || "No reason provided", inline: true },
                { name: "🏠 Server", value: guild.name, inline: true },
                { name: "⏰ Time", value: `<t:${Math.floor(Date.now()/1000)}:R>`, inline: true }
            ])
            .setFooter({ text: "ACS Blacklist System" })
            .setTimestamp();
            
            await owner.send({ embeds: [ownerDM] });
            console.log(`DM sent to server owner ${owner.user.tag}`);
            
        } catch (ownerError) {
            console.log(`Could not DM server owner (DMs closed):`, ownerError.message);
            
            // Only attempt to ping in channel on first strike if DMs are closed
            if (strikeNumber === 1) {
                const dmAttempted = await hasOwnerDMBeenAttempted(guildId);
                
                if (!dmAttempted) {
                    // First time owner DMs failed - send channel message asking to open DMs
                    try {
                        const channel = await findFirstTextChannel(guild);
                        if (channel) {
                            const channelNotification = new EmbedBuilder()
                            .setTitle(`${BOT_EMOJI} FIRST STRIKE ISSUED`)
                            .setColor(0xFFA500)
                            .setDescription(`This server **${guild.name}** has received strike **1** for unbanning a blacklisted user.`)
                            .addFields([
                                { name: "👤 User Unbanned", value: `${user.tag} (${userId})`, inline: true },
                                { name: "🛡️ Unbanned By", value: executorDisplay, inline: true },
                                { name: "📋 Blacklist Reason", value: blacklistReason || "No reason provided", inline: true },
                                { name: "⏰ Time", value: `<t:${Math.floor(Date.now()/1000)}:R>`, inline: true },
                                { name: "📢 IMPORTANT - ACTION REQUIRED", 
                                    value: `<@${guild.ownerId}> Please **enable DMs from server members** so I can contact you directly about urgent issues like this. Continued violations may result in your server being blacklisted from using ACS.`, 
                                    inline: false }
                            ])
                            .setFooter({ text: "ACS Blacklist System" })
                            .setTimestamp();
                            
                            await channel.send({ content: `<@${guild.ownerId}>`, embeds: [channelNotification] });
                            await markOwnerDMAsAttempted(guildId);
                            console.log(`Sent first strike channel notification to owner in ${guild.name}`);
                        }
                    } catch (channelError) {
                        console.log(`Could not notify in server channel:`, channelError.message);
                    }
                } else {
                    console.log(`Owner DM already attempted for guild ${guildId}, skipping channel notification`);
                }
            } else {
                // For strikes after 1, just log that we couldn't DM but don't send channel message
                console.log(`Strike ${strikeNumber} - Owner DMs still closed, continuing without channel notification`);
            }
        }
        
        // 3. Also log to the main log channel
        await logGlobal(notificationEmbed);
        console.log(`Strike ${strikeNumber} logged to global channel`);
        
    } catch (error) {
        console.error("Error sending strike notifications:", error);
    }
}

// ================= IMPROVED ERROR RECOVERY =================
async function safeBan(guild, userId, reason) {
    const startTime = Date.now();
    for (let attempt = 1; attempt <= 3; attempt++) {
        try {
            await guild.members.ban(userId, { reason });
            const duration = Date.now() - startTime;
            trackPerformance('ban', duration);
            performance.bansExecuted++;
            performance.successes++;
            updateDailyStats('bans');
            return true;
        } catch (error) {
            performance.errors++;
            
            if (error.code === 50013) { // Missing permissions
                console.log(`Missing permissions to ban ${userId} in ${guild.name}`);
                return false;
            }
            
            if (error.code === 50007) { // Cannot send messages
                console.log(`Cannot send messages in ${guild.name}`);
                return false;
            }
            
            if (error.code === 50001) { // Missing access
                console.log(`Missing access to ${guild.name}`);
                return false;
            }
            
            if (attempt === 3) {
                console.error(`Failed to ban ${userId} after 3 attempts:`, error.message);
                throw error;
            }
            
            // Exponential backoff
            const delay = 1000 * Math.pow(2, attempt - 1);
            console.log(`Retry ${attempt} for ${userId} in ${guild.name} after ${delay}ms`);
            await new Promise(r => setTimeout(r, delay));
        }
    }
    return false;
}

async function safeUnban(guild, userId, reason) {
    const startTime = Date.now();
    for (let attempt = 1; attempt <= 3; attempt++) {
        try {
            await guild.members.unban(userId, reason);
            const duration = Date.now() - startTime;
            trackPerformance('unban', duration);
            performance.unbansExecuted++;
            performance.successes++;
            updateDailyStats('unbans');
            return true;
        } catch (error) {
            performance.errors++;
            
            if (error.code === 10026) { // Unknown ban
                // User isn't banned, that's okay
                return true;
            }
            
            if (error.code === 50013) { // Missing permissions
                console.log(`Missing permissions to unban ${userId} in ${guild.name}`);
                return false;
            }
            
            if (attempt === 3) {
                console.error(`Failed to unban ${userId} after 3 attempts:`, error.message);
                return false;
            }
            
            const delay = 1000 * Math.pow(2, attempt - 1);
            await new Promise(r => setTimeout(r, delay));
        }
    }
    return false;
}

// ================= MODIFIED STRIKE MANAGEMENT (UNLIMITED STRIKES) =================
async function addStrike(guildId, userId, executor, executorId, executorType) {
    const startTime = Date.now();
    const guild = client.guilds.cache.get(guildId);
    const guildName = guild ? guild.name : "Unknown Guild";
    
    // Check if server is blacklisted - if so, do NOT add strikes
    const isBlacklisted = await isServerBlacklisted(guildId);
    if (isBlacklisted) {
        console.log(`Guild ${guildName} is blacklisted - skipping strike addition (COMPLETE RADIO SILENCE)`);
        return { strikeId: null, count: 0, skipped: true };
    }
    
    return new Promise((resolve, reject) => {
        // First, ensure the count column exists by trying to add it (ignoring error if it exists)
        db.run(`ALTER TABLE strikes ADD COLUMN count INTEGER DEFAULT 1`, (alterErr) => {
            if (alterErr) {
                // Column probably already exists, ignore
            }
            
            // Now get current strike count for this guild
            db.get(`SELECT id, count FROM strikes WHERE guildId = ?`, [guildId], async (err, row) => {
                if (err) {
                    console.error("Error checking strikes:", err);
                    return reject(err);
                }
                
                // Get blacklist reason for this user
                let blacklistReason = "Unknown";
                db.get(`SELECT reason FROM blacklist WHERE userId = ?`, [userId], (err, reasonRow) => {
                    if (!err && reasonRow) blacklistReason = reasonRow.reason;
                });
                    
                if (!row) {
                    // First strike for this guild
                    db.run(`INSERT INTO strikes (guildId, guildName, userId, executor, executorId, executorType, timestamp, count) VALUES (?, ?, ?, ?, ?, ?, ?, 1)`,
                           [guildId, guildName, userId, executor, executorId, executorType, Date.now()], async function(insertErr) {
                               if (insertErr) {
                                   console.error("Error inserting first strike:", insertErr);
                                   return reject(insertErr);
                               }
                               
                               const strikeId = this.lastID;
                               console.log(`First strike added for guild ${guildId} by ${executor} (${executorType})`);
                               
                               await logStrikeAction(guildId, guildName, strikeId, "STRIKE_ADDED", 
                                                     `Strike #1 - Unban attempt by ${executor} on ${userId}`);
                               
                               await logStrikeToChannel(guildId, guildName, userId, executor, executorId, executorType, 1, strikeId);
                               
                               // Send notifications for first strike
                               await sendStrikeNotifications(guildId, userId, executor, executorId, executorType, 1, blacklistReason);
                               
                               const duration = Date.now() - startTime;
                               trackPerformance('strike', duration);
                               performance.strikesIssued++;
                               updateDailyStats('strikes');
                               
                               resolve({ strikeId, count: 1 });
                           }
                    );
                } else {
                    const currentCount = row.count || 1;
                    const newCount = currentCount + 1;
                    
                    // Update strike count (unlimited - no max)
                    db.run(`UPDATE strikes SET count = ?, executor = ?, executorId = ?, executorType = ?, timestamp = ?, guildName = ? WHERE guildId = ?`,
                           [newCount, executor, executorId, executorType, Date.now(), guildName, guildId], async function(updateErr) {
                               if (updateErr) {
                                   console.error("Error updating strike:", updateErr);
                                   return reject(updateErr);
                               }
                               
                               const strikeId = row.id;
                               console.log(`Strike #${newCount} added for guild ${guildId} by ${executor} (${executorType})`);
                               
                               await logStrikeAction(guildId, guildName, strikeId, "STRIKE_ADDED",
                                                     `Strike #${newCount} - Unban attempt by ${executor} on ${userId}`);
                               
                               await logStrikeToChannel(guildId, guildName, userId, executor, executorId, executorType, newCount, strikeId);
                               
                               // Send notifications for this strike
                               await sendStrikeNotifications(guildId, userId, executor, executorId, executorType, newCount, blacklistReason);
                               
                               const duration = Date.now() - startTime;
                               trackPerformance('strike', duration);
                               performance.strikesIssued++;
                               updateDailyStats('strikes');
                               
                               resolve({ strikeId, count: newCount });
                           }
                    );
                }
            });
        });
    });
}

async function logStrikeAction(guildId, guildName, strikeId, action, details) {
    return new Promise((resolve, reject) => {
        db.run(
            `INSERT INTO strike_logs (guildId, guildName, strikeId, action, details, timestamp) VALUES (?, ?, ?, ?, ?, ?)`,
            [guildId, guildName, strikeId, action, details, Date.now()],
            function(err) {
                if (err) reject(err);
                else resolve(this.lastID);
            }
        );
    });
}

async function logStrikeToChannel(guildId, guildName, userId, executor, executorId, executorType, strikeNumber, strikeId) {
    try {
        const guild = client.guilds.cache.get(guildId);
        
        // Check if server is blacklisted - if so, do NOT log to global channel
        const isBlacklisted = await isServerBlacklisted(guildId);
        if (isBlacklisted) {
            console.log(`Guild ${guildName} is blacklisted - skipping global log (COMPLETE RADIO SILENCE)`);
            return;
        }
        
        const user = await client.users.fetch(userId).catch(() => ({ tag: "Unknown User" }));
        
        // Format executor display
        const executorDisplay = executorType === 'bot' ? `${executor} (via bot command)` : executor;
        
        const embed = new EmbedBuilder()
        .setTitle(`${BOT_EMOJI} Strike #${strikeNumber} Issued`)
        .setColor(strikeNumber === 1 ? 0xFFA500 : strikeNumber === 2 ? 0xFF6600 : 0xFF0000)
        .setDescription(`**Guild:** ${guildName} (${guildId})`)
        .addFields([
            { name: "Strike ID", value: `#${strikeId}`, inline: true },
            { name: "Target User", value: `${user.tag} (${userId})`, inline: true },
            { name: "Unbanned By", value: executorDisplay, inline: true },
            { name: "Executor Type", value: executorType, inline: true },
            { name: "Strike Count", value: `${strikeNumber}`, inline: true },
            { name: "Timestamp", value: `<t:${Math.floor(Date.now()/1000)}:R>`, inline: true }
        ])
        .setFooter({ text: `ACS Blacklist System • Strike #${strikeNumber}` })
        .setTimestamp();
        
        await logGlobal(embed);
    } catch (error) {
        console.error("Failed to log strike to channel:", error);
    }
}

// ================= BLACKLIST ENFORCEMENT =================
async function enforceBlacklist() {
    const startTime = Date.now();
    return new Promise((resolve, reject) => {
        db.all(`SELECT * FROM blacklist`, async (err, users) => {
            if (err) return reject(err);
            
            const results = {
                banned: [],
                failed: [],
                skipped: []
            };
            
            // Process each guild
            const guilds = Array.from(client.guilds.cache.values());
            
            for (const guild of guilds) {
                // Check if guild is blacklisted - if so, skip ENTIRELY - do NOTHING in this guild
                const isBlacklisted = await isServerBlacklisted(guild.id);
                if (isBlacklisted) {
                    results.skipped.push({ guildId: guild.id, guildName: guild.name, reason: "Server is blacklisted - COMPLETE RADIO SILENCE" });
                    console.log(`Guild ${guild.name} is blacklisted - skipping ALL enforcement actions (COMPLETE RADIO SILENCE)`);
                    continue;
                }
                
                // Check if guild is exempt
                if (config.exemptGuilds?.includes(guild.id)) {
                    results.skipped.push({ guildId: guild.id, guildName: guild.name, reason: "Exempt guild" });
                    continue;
                }
                
                // Process each blacklisted user in this guild
                for (const user of users) {
                    try {
                        // Check if user is already banned
                        const bans = await guild.bans.fetch().catch(() => new Map());
                        if (bans.has(user.userId)) {
                            continue;
                        }
                        
                        // Ban with improved error recovery
                        const success = await safeBan(guild, user.userId, `[ACS] ${user.reason}`);
                        
                        if (success) {
                            results.banned.push({ 
                                userId: user.userId, 
                                guildName: guild.name,
                                guildId: guild.id 
                            });
                        } else {
                            results.failed.push({ 
                                userId: user.userId, 
                                guildName: guild.name,
                                reason: "Failed after retries",
                                guildId: guild.id
                            });
                        }
                        
                    } catch (error) {
                        results.failed.push({ 
                            userId: user.userId, 
                            guildName: guild.name,
                            reason: error.message,
                            guildId: guild.id
                        });
                    }
                }
            }
            
            const duration = Date.now() - startTime;
            trackPerformance('enforce_blacklist', duration);
            console.log(`Blacklist enforcement completed in ${duration}ms`);
            
            resolve(results);
        });
    });
}

async function globalUnban(userId) {
    const guilds = Array.from(client.guilds.cache.values());
    
    for (const guild of guilds) {
        try {
            // Check if guild is blacklisted - if so, skip entirely
            const isBlacklisted = await isServerBlacklisted(guild.id);
            if (isBlacklisted) {
                console.log(`Guild ${guild.name} is blacklisted - skipping unban (COMPLETE RADIO SILENCE)`);
                continue;
            }
            
            // Check if user is banned
            const bans = await guild.bans.fetch().catch(() => new Map());
            if (!bans.has(userId)) continue;
            
            // Unban with improved error recovery
            await safeUnban(guild, userId, "ACS unblacklist");
            
            await new Promise(r => setTimeout(r, 100)); // Small delay between unbans
        } catch (error) {
            console.error(`Failed to unban ${userId} from ${guild.name}:`, error);
        }
    }
}

// ================= PAGINATION FUNCTION =================
async function sendPaginatedBlacklist(interaction, page = 1) {
    const itemsPerPage = 10;
    const offset = (page - 1) * itemsPerPage;
    
    try {
        // Get total count
        const totalCount = await new Promise((resolve, reject) => {
            db.get(`SELECT COUNT(*) as count FROM blacklist`, (err, row) => {
                if (err) reject(err);
                else resolve(row ? row.count : 0);
            });
        });
        
        if (totalCount === 0) {
            return interaction.editReply({ content: "No blacklisted users found.", components: [] });
        }
        
        const totalPages = Math.ceil(totalCount / itemsPerPage);
        
        if (page > totalPages) {
            page = totalPages;
        }
        
        // Get blacklisted users for current page
        const users = await new Promise((resolve, reject) => {
            db.all(`SELECT * FROM blacklist ORDER BY addedAt DESC LIMIT ? OFFSET ?`, [itemsPerPage, offset], (err, rows) => {
                if (err) reject(err);
                else resolve(rows || []);
            });
        });
        
        const embed = new EmbedBuilder()
        .setTitle(`${BOT_EMOJI} Blacklisted Users`)
        .setColor(0xFF0000)
        .setDescription(`Total blacklisted users: **${totalCount}**`)
        .setFooter({ text: `Page ${page}/${totalPages}` })
        .setTimestamp();
        
        for (const user of users) {
            try {
                const discordUser = await client.users.fetch(user.userId).catch(() => null);
                const username = discordUser ? discordUser.tag : "Unknown User";
                const addedBy = await client.users.fetch(user.addedBy).catch(() => ({ tag: "Unknown Moderator" }));
                
                embed.addFields({
                    name: `${username} (${user.userId})`,
                    value: `**Reason:** ${user.reason}\n**Added by:** ${addedBy.tag}\n**Date:** <t:${Math.floor(user.addedAt/1000)}:R>`,
                    inline: false
                });
            } catch (error) {
                embed.addFields({
                    name: `Unknown User (${user.userId})`,
                    value: `**Reason:** ${user.reason}\n**Added by:** <@${user.addedBy}>\n**Date:** <t:${Math.floor(user.addedAt/1000)}:R>`,
                    inline: false
                });
            }
        }
        
        // Create navigation buttons
        const row = new ActionRowBuilder()
        .addComponents(
            new ButtonBuilder()
            .setCustomId('first_page')
            .setLabel('⏪ First')
            .setStyle(ButtonStyle.Primary)
            .setDisabled(page === 1),
            new ButtonBuilder()
            .setCustomId('prev_page')
            .setLabel('◀ Previous')
            .setStyle(ButtonStyle.Primary)
            .setDisabled(page === 1),
            new ButtonBuilder()
            .setCustomId('page_indicator')
            .setLabel(`${page}/${totalPages}`)
            .setStyle(ButtonStyle.Secondary)
            .setDisabled(true),
            new ButtonBuilder()
            .setCustomId('next_page')
            .setLabel('Next ▶')
            .setStyle(ButtonStyle.Primary)
            .setDisabled(page === totalPages),
            new ButtonBuilder()
            .setCustomId('last_page')
            .setLabel('Last ⏩')
            .setStyle(ButtonStyle.Primary)
            .setDisabled(page === totalPages)
        );
        
        await interaction.editReply({ embeds: [embed], components: [row] });
        
        return { totalPages, currentPage: page };
    } catch (error) {
        console.error("Error in pagination:", error);
        await interaction.editReply({ content: "An error occurred while fetching blacklisted users.", components: [] });
    }
}

// ================= EXPORT/IMPORT SYSTEM =================
async function exportBlacklist(format) {
    return new Promise((resolve, reject) => {
        db.all(`SELECT * FROM blacklist`, (err, rows) => {
            if (err) return reject(err);
            
            if (format === "json") {
                resolve(JSON.stringify(rows, null, 2));
            } else if (format === "csv") {
                const csv = rows.map(row => 
                    `${row.userId},"${(row.reason || '').replace(/"/g, '""')}",${row.addedAt},${row.addedBy}`
                ).join("\n");
                resolve("userId,reason,addedAt,addedBy\n" + csv);
            } else {
                reject(new Error("Invalid format"));
            }
        });
    });
}

async function importBlacklist(data, format, importerId) {
    return new Promise((resolve, reject) => {
        try {
            let users = [];
            
            if (format === "json") {
                users = JSON.parse(data);
            } else if (format === "csv") {
                const lines = data.split('\n');
                const headers = lines[0].split(',');
                
                for (let i = 1; i < lines.length; i++) {
                    if (!lines[i].trim()) continue;
                    
                    // Simple CSV parsing
                    const values = lines[i].split(/,(?=(?:(?:[^"]*"){2})*[^"]*$)/);
                    const user = {
                        userId: values[0],
                        reason: values[1] ? values[1].replace(/^"|"$/g, '') : '',
                        addedAt: parseInt(values[2]) || Date.now(),
                        addedBy: values[3] || importerId
                    };
                    users.push(user);
                }
            }
            
            // Validate and import
            const validUsers = users.filter(user => user.userId);
            
            if (validUsers.length === 0) {
                return reject(new Error("No valid users found in import data"));
            }
            
            // Begin transaction
            db.run("BEGIN TRANSACTION");
            
            validUsers.forEach(user => {
                db.run(`INSERT OR REPLACE INTO blacklist VALUES (?, ?, ?, ?)`,
                       [user.userId, user.reason || 'Imported', user.addedAt || Date.now(), user.addedBy || importerId]);
            });
            
            db.run("COMMIT", (err) => {
                if (err) {
                    db.run("ROLLBACK");
                    reject(err);
                } else {
                    resolve({ total: users.length, valid: validUsers.length, invalid: users.length - validUsers.length });
                }
            });
            
        } catch (error) {
            reject(error);
        }
    });
}

// ================= BACKUP SYSTEM =================
async function backupDatabase() {
    const backupDir = './backups';
    try {
        await fs.mkdir(backupDir, { recursive: true });
        
        const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
        const backupFile = `${backupDir}/blacklist_${timestamp}.db`;
        
        await fs.copyFile('./blacklist.db', backupFile);
        console.log(`Backup created: ${backupFile}`);
        
        // Keep only last 7 backups
        const files = await fs.readdir(backupDir);
        const backupFiles = files.filter(f => f.startsWith('blacklist_') && f.endsWith('.db'));
        
        if (backupFiles.length > 7) {
            backupFiles.sort();
            const filesToDelete = backupFiles.slice(0, backupFiles.length - 7);
            
            for (const file of filesToDelete) {
                await fs.unlink(path.join(backupDir, file));
                console.log(`Deleted old backup: ${file}`);
            }
        }
        
        return backupFile;
    } catch (error) {
        console.error("Backup failed:", error);
        return null;
    }
}

// ================= INTERACTIONS =================
client.on("interactionCreate", async i => {
    if (i.isChatInputCommand()) {
        if (i.guildId !== config.controlGuildId)
            return i.reply({ content: "Commands can only be used in the control guild.", flags: 64 });
        if (!isAuthorized(i.member))
            return i.reply({ content: "You don't have permission to use this command.", flags: 64 });
        
        // ================= BLACKLIST ADD SUBCOMMAND =================
        if (i.commandName === "blacklist" && i.options.getSubcommand() === "add") {
            await i.deferReply();
            
            const userId = i.options.getString("userid");
            const reason = i.options.getString("reason");
            
            try {
                await client.users.fetch(userId);
            } catch {
                return i.editReply({ content: "User not found. Please check the user ID." });
            }
            
            db.run(`INSERT OR REPLACE INTO blacklist VALUES (?, ?, ?, ?)`, 
                   [userId, reason, Date.now(), i.user.id]);
            
            logAction(userId, "BLACKLISTED", `Reason: ${reason} | By: ${i.user.tag}`);
            
            // Create embed with processing message
            const embed = new EmbedBuilder()
            .setTitle(`${BOT_EMOJI} User Blacklisted`)
            .setColor(0xff0000)
            .setDescription(`<@${userId}> has been blacklisted globally`)
            .addFields([
                { name: "User ID", value: userId, inline: true },
                { name: "Username", value: (await client.users.fetch(userId)).tag, inline: true },
                { name: "Reason", value: reason, inline: true },
                { name: "Moderator", value: `<@${i.user.id}>`, inline: true },
                { name: "Bans Applied", value: "Processing...", inline: true },
                { name: "DM Status", value: "Pending...", inline: true }
            ])
            .setFooter({ text: "ACS Blacklist System" })
            .setTimestamp();
            
            await i.editReply({ embeds: [embed] });
            
            // Enforce blacklist and get results
            const result = await enforceBlacklist();
            
            // Send DM to the user
            const dmSent = await sendBlacklistDM(userId, reason, result.banned.length, i.user.id);
            
            // Update the embed with actual results
            embed.data.fields[4].value = `${result.banned.length} server(s)`;
            embed.data.fields[5].value = dmSent ? "✅ DM sent" : "❌ Could not send DM";
            
            // Update the message
            await i.editReply({ embeds: [embed] });
            
            // Log to global channel
            await logGlobal(embed);
            
            // If there were failures, send a follow-up
            if (result.failed.length > 0) {
                let failMsg = `**Failed bans (${result.failed.length}):**\n`;
                result.failed.slice(0, 5).forEach(f => {
                    failMsg += `• ${f.guildName}: ${f.reason}\n`;
                });
                if (result.failed.length > 5) failMsg += `...and ${result.failed.length - 5} more`;
                
                await i.followUp({ content: failMsg, flags: 64 });
            }
        }
        
        // ================= BLACKLIST SERVER SUBCOMMAND =================
        if (i.commandName === "blacklist" && i.options.getSubcommand() === "server") {
            await i.deferReply();
            
            const guildId = i.options.getString("guildid");
            const reason = i.options.getString("reason");
            
            // Check if server exists
            const guild = client.guilds.cache.get(guildId);
            
            db.get(`SELECT * FROM server_blacklist WHERE guildId = ?`, [guildId], async (err, row) => {
                if (err) {
                    console.error("Database error:", err);
                    return i.editReply({ content: "Database error occurred." });
                }
                
                if (row) {
                    return i.editReply({ content: `Server ${guildId} is already blacklisted.` });
                }
                
                // Add to server blacklist
                db.run(`INSERT INTO server_blacklist (guildId, reason, addedAt, addedBy, notified) VALUES (?, ?, ?, ?, 0)`,
                       [guildId, reason, Date.now(), i.user.id]);
                
                logAction(guildId, "SERVER_BLACKLISTED", `Reason: ${reason} | By: ${i.user.id}`);
                
                const embed = new EmbedBuilder()
                .setTitle(`${BOT_EMOJI} Server Blacklisted`)
                .setColor(0xff0000)
                .setDescription(`Server **${guild?.name || 'Unknown'}** (${guildId}) has been blacklisted`)
                .addFields([
                    { name: "Server ID", value: guildId, inline: true },
                    { name: "Server Name", value: guild?.name || 'Unknown', inline: true },
                    { name: "Reason", value: reason, inline: true },
                    { name: "Moderator", value: `<@${i.user.id}>`, inline: true }
                ])
                .setFooter({ text: "ACS Blacklist System" })
                .setTimestamp();
                
                await i.editReply({ embeds: [embed] });
                await logGlobal(embed);
                
                // DO NOT LEAVE THE GUILD - just let it sit there doing nothing
                console.log(`Server ${guild?.name || guildId} has been blacklisted - bot will now be COMPLETELY SILENT in this server`);
            });
        }
        
        // ================= BLACKLIST SERVERLIST SUBCOMMAND =================
        if (i.commandName === "blacklist" && i.options.getSubcommand() === "serverlist") {
            await i.deferReply();
            
            const page = i.options.getInteger("page") || 1;
            const itemsPerPage = 10;
            const offset = (page - 1) * itemsPerPage;
            
            try {
                // Get total count
                const totalCount = await new Promise((resolve, reject) => {
                    db.get(`SELECT COUNT(*) as count FROM server_blacklist`, (err, row) => {
                        if (err) reject(err);
                        else resolve(row ? row.count : 0);
                    });
                });
                
                if (totalCount === 0) {
                    return i.editReply({ content: "No blacklisted servers found." });
                }
                
                const totalPages = Math.ceil(totalCount / itemsPerPage);
                
                if (page > totalPages) {
                    return i.editReply({ content: `Page ${page} doesn't exist. Total pages: ${totalPages}` });
                }
                
                // Get blacklisted servers for current page
                const servers = await new Promise((resolve, reject) => {
                    db.all(`SELECT * FROM server_blacklist ORDER BY addedAt DESC LIMIT ? OFFSET ?`, [itemsPerPage, offset], (err, rows) => {
                        if (err) reject(err);
                        else resolve(rows || []);
                    });
                });
                
                const embed = new EmbedBuilder()
                .setTitle(`${BOT_EMOJI} Blacklisted Servers`)
                .setColor(0xFF0000)
                .setDescription(`Total blacklisted servers: **${totalCount}**`)
                .setFooter({ text: `Page ${page}/${totalPages}` })
                .setTimestamp();
                
                for (const server of servers) {
                    const guild = client.guilds.cache.get(server.guildId);
                    const guildName = guild ? guild.name : "Unknown Server";
                    const addedBy = await client.users.fetch(server.addedBy).catch(() => ({ tag: "Unknown Moderator" }));
                    
                    embed.addFields({
                        name: `${guildName} (${server.guildId})`,
                        value: `**Reason:** ${server.reason}\n**Added by:** ${addedBy.tag}\n**Date:** <t:${Math.floor(server.addedAt/1000)}:R>\n**Status:** ${guild ? 'Bot still present (silent)' : 'Bot not in server'}`,
                        inline: false
                    });
                }
                
                await i.editReply({ embeds: [embed] });
                
            } catch (error) {
                console.error("Error in serverlist command:", error);
                await i.editReply({ content: "An error occurred while fetching blacklisted servers." });
            }
        }
        
        // ================= BLACKLIST LIST SUBCOMMAND (WITH PAGINATION) =================
        if (i.commandName === "blacklist" && i.options.getSubcommand() === "list") {
            await i.deferReply();
            
            const page = i.options.getInteger("page") || 1;
            await sendPaginatedBlacklist(i, page);
        }
        
        // ================= SEARCH COMMAND =================
        if (i.commandName === "search") {
            await i.deferReply({ flags: 64 });
            
            const userId = i.options.getString("userid");
            
            try {
                // Search for the user in blacklist
                const user = await new Promise((resolve, reject) => {
                    db.get(`SELECT * FROM blacklist WHERE userId = ?`, [userId], (err, row) => {
                        if (err) reject(err);
                        else resolve(row);
                    });
                });
                
                if (!user) {
                    return i.editReply({ content: `User \`${userId}\` is not in the blacklist.` });
                }
                
                // Get user info
                const discordUser = await client.users.fetch(user.userId).catch(() => null);
                const username = discordUser ? discordUser.tag : "Unknown User";
                const addedBy = await client.users.fetch(user.addedBy).catch(() => ({ tag: "Unknown Moderator" }));
                
                // Get all guilds where this user is banned
                const guildsBanned = [];
                for (const guild of client.guilds.cache.values()) {
                    const isBlacklisted = await isServerBlacklisted(guild.id);
                    if (isBlacklisted) continue;
                    
                    try {
                        const bans = await guild.bans.fetch().catch(() => new Map());
                        if (bans.has(user.userId)) {
                            guildsBanned.push(guild.name);
                        }
                    } catch (error) {
                        // Skip guilds with errors
                    }
                }
                
                const embed = new EmbedBuilder()
                .setTitle(`${BOT_EMOJI} Blacklist Search Result`)
                .setColor(0xFF0000)
                .addFields([
                    { name: "User ID", value: userId, inline: true },
                    { name: "Username", value: username, inline: true },
                    { name: "Reason", value: user.reason, inline: false },
                    { name: "Added By", value: `${addedBy.tag} (<@${user.addedBy}>)`, inline: true },
                    { name: "Added Date", value: `<t:${Math.floor(user.addedAt/1000)}:F>`, inline: true },
                    { name: "Banned In", value: guildsBanned.length > 0 ? `${guildsBanned.length} servers` : "No servers (bot may not have permissions)", inline: true }
                ])
                .setFooter({ text: "ACS Blacklist System" })
                .setTimestamp();
                
                if (guildsBanned.length > 0) {
                    const guildList = guildsBanned.slice(0, 5).join(", ");
                    embed.addFields({ name: "Server List", value: guildList + (guildsBanned.length > 5 ? ` and ${guildsBanned.length - 5} more` : ""), inline: false });
                }
                
                await i.editReply({ embeds: [embed] });
                
            } catch (error) {
                console.error("Error in search command:", error);
                await i.editReply({ content: "An error occurred while searching." });
            }
        }
        
        // ================= UNBLACKLIST USER SUBCOMMAND =================
        if (i.commandName === "unblacklist" && i.options.getSubcommand() === "user") {
            await i.deferReply();
            
            const userId = i.options.getString("userid");
            const reason = i.options.getString("reason");
            
            // Check if user is actually blacklisted
            db.get(`SELECT * FROM blacklist WHERE userId = ?`, [userId], async (err, row) => {
                if (err || !row) {
                    return i.editReply({ content: "User is not blacklisted." });
                }
                
                db.run(`DELETE FROM blacklist WHERE userId = ?`, [userId]);
                logAction(userId, "UNBLACKLISTED", `Reason: ${reason} | By: ${i.user.tag}`);
                await globalUnban(userId);
                
                const embed = new EmbedBuilder()
                .setTitle(`${BOT_EMOJI} User Unblacklisted`)
                .setColor(0x00ff00)
                .setDescription(`<@${userId}> has been removed from the blacklist`)
                .addFields([
                    { name: "User ID", value: userId, inline: true },
                    { name: "Username", value: (await client.users.fetch(userId).catch(() => ({ tag: "Unknown" }))).tag, inline: true },
                    { name: "Reason", value: reason, inline: true },
                    { name: "Moderator", value: `<@${i.user.id}>`, inline: true }
                ])
                .setFooter({ text: "ACS Blacklist System" })
                .setTimestamp();
                
                await i.editReply({ embeds: [embed] });
                await logGlobal(embed);
            });
        }
        
        // ================= UNBLACKLIST SERVER SUBCOMMAND =================
        if (i.commandName === "unblacklist" && i.options.getSubcommand() === "server") {
            await i.deferReply();
            
            const guildId = i.options.getString("guildid");
            const reason = i.options.getString("reason");
            
            db.get(`SELECT * FROM server_blacklist WHERE guildId = ?`, [guildId], async (err, row) => {
                if (err) {
                    console.error("Database error:", err);
                    return i.editReply({ content: "Database error occurred." });
                }
                
                if (!row) {
                    return i.editReply({ content: `Server ${guildId} is not blacklisted.` });
                }
                
                // Remove from server blacklist
                db.run(`DELETE FROM server_blacklist WHERE guildId = ?`, [guildId]);
                
                logAction(guildId, "SERVER_UNBLACKLISTED", `Reason: ${reason} | By: ${i.user.id}`);
                
                const embed = new EmbedBuilder()
                .setTitle(`${BOT_EMOJI} Server Unblacklisted`)
                .setColor(0x00ff00)
                .setDescription(`Server ${guildId} has been removed from the server blacklist`)
                .addFields([
                    { name: "Server ID", value: guildId, inline: true },
                    { name: "Reason", value: reason, inline: true },
                    { name: "Moderator", value: `<@${i.user.id}>`, inline: true }
                ])
                .setFooter({ text: "ACS Blacklist System" })
                .setTimestamp();
                
                await i.editReply({ embeds: [embed] });
                await logGlobal(embed);
                
                const guild = client.guilds.cache.get(guildId);
                if (guild) {
                    console.log(`Server ${guild.name} has been unblacklisted - bot will resume normal operation`);
                }
            });
        }
        
        // ================= MODLOGS COMMAND =================
        if (i.commandName === "modlogs") {
            await i.deferReply({ flags: 64 });
            
            const userId = i.options.getString("userid");
            
            try {
                // Convert db.all to promise
                const rows = await new Promise((resolve, reject) => {
                    db.all(`SELECT * FROM modlogs WHERE userId = ? ORDER BY timestamp DESC LIMIT 50`, [userId], (err, rows) => {
                        if (err) reject(err);
                        else resolve(rows);
                    });
                });
                
                if (!rows || rows.length === 0) {
                    return await i.editReply({ content: "No logs found for this user." });
                }
                
                const chunks = [];
                let currentChunk = "";
                
                rows.forEach(r => {
                    const line = `**${r.action}**: ${r.details || "-"} | <t:${Math.floor(r.timestamp/1000)}:R>\n`;
                    if (currentChunk.length + line.length > 4000) {
                        chunks.push(currentChunk);
                        currentChunk = line;
                    } else {
                        currentChunk += line;
                    }
                });
                if (currentChunk) chunks.push(currentChunk);
                
                for (let j = 0; j < chunks.length; j++) {
                    const embed = new EmbedBuilder()
                    .setTitle(`${BOT_EMOJI} ACS Modlogs for ${j === 0 ? `<@${userId}>` : 'Continued...'}`)
                    .setColor(0x00ffff)
                    .setDescription(chunks[j])
                    .setFooter({ text: `Page ${j + 1}/${chunks.length}` })
                    .setTimestamp();
                    
                    if (j === 0) {
                        await i.editReply({ embeds: [embed] });
                    } else {
                        await i.followUp({ embeds: [embed], flags: 64 });
                    }
                }
            } catch (err) {
                console.error("Error in modlogs:", err);
                await i.editReply({ content: "Database error occurred." });
            }
        }
        
        // ================= STRIKES COMMAND =================
        if (i.commandName === "strikes") {
            await i.deferReply({ flags: 64 });
            
            const guildId = i.options.getString("guildid") || i.guildId;
            
            // Check if the requested guild is blacklisted - if so, return nothing
            const isBlacklisted = await isServerBlacklisted(guildId);
            if (isBlacklisted) {
                return i.editReply({ content: "This server is blacklisted - no strike information available." });
            }
            
            db.all(`SELECT * FROM strikes WHERE guildId = ? ORDER BY timestamp DESC`, [guildId], async (err, strikes) => {
                if (err) {
                    console.error("Database error in strikes:", err);
                    return i.editReply({ content: "Database error occurred." });
                }
                
                if (!strikes || strikes.length === 0) {
                    return i.editReply({ content: "No strikes found for this guild." });
                }
                
                const guild = client.guilds.cache.get(guildId);
                const embed = new EmbedBuilder()
                .setTitle(`${BOT_EMOJI} Strikes for ${guild?.name || 'Unknown Guild'}`)
                .setColor(0xFFA500)
                .setDescription(`Total Strikes: ${strikes.length}`)
                .setFooter({ text: "ACS Strike System" })
                .setTimestamp();
                
                for (const strike of strikes) {
                    const logs = await new Promise((resolve) => {
                        db.all(`SELECT details FROM strike_logs WHERE strikeId = ? ORDER BY timestamp DESC LIMIT 1`, 
                               [strike.id], (err, logs) => {
                                   if (err || !logs) resolve([]);
                                   else resolve(logs);
                               });
                    });
                    
                    const details = logs[0]?.details || "No details";
                    const executorDisplay = strike.executorType === 'bot' ? `${strike.executor} (via bot)` : strike.executor;
                    
                    embed.addFields({
                        name: `Strike #${strike.count}`,
                        value: `**User:** <@${strike.userId}>\n**Unbanned By:** ${executorDisplay}\n**Server:** ${strike.guildName || 'Unknown'}\n**Details:** ${details}\n**Time:** <t:${Math.floor(strike.timestamp/1000)}:R>`,
                        inline: false
                    });
                }
                
                await i.editReply({ embeds: [embed] });
            });
        }
        
        // ================= RELOAD COMMAND =================
        if (i.commandName === "reload") {
            await i.deferReply();
            
            const result = await enforceBlacklist();
            
            const embed = new EmbedBuilder()
            .setTitle(`${BOT_EMOJI} Blacklist Reload Complete`)
            .setColor(0x00ff00)
            .setDescription(`Processed ${client.guilds.cache.size} guilds`)
            .addFields([
                { name: "✅ Successful Bans", value: result.banned.length.toString(), inline: true },
                { name: "⚠ Failed Bans", value: result.failed.length.toString(), inline: true },
                { name: "⏭ Skipped Guilds", value: result.skipped.length.toString(), inline: true }
            ])
            .setFooter({ text: "ACS Blacklist System" })
            .setTimestamp();
            
            await i.editReply({ embeds: [embed] });
            
            // Send detailed log if there are failures
            if (result.failed.length > 0) {
                let details = "**Failed Bans:**\n";
                result.failed.slice(0, 10).forEach(f => {
                    details += `• ${f.userId} in ${f.guildName}: ${f.reason}\n`;
                });
                if (result.failed.length > 10) details += `...and ${result.failed.length - 10} more`;
                
                await i.followUp({ content: details, flags: 64 });
            }
        }
        
        // ================= PERFORMANCE COMMAND =================
        if (i.commandName === "performance") {
            await i.deferReply({ flags: 64 });
            
            // Get recent performance stats
            db.all(`SELECT action, AVG(duration) as avg_duration, COUNT(*) as count 
            FROM performance_stats 
            WHERE timestamp > ? 
            GROUP BY action`, 
            [Date.now() - 24 * 60 * 60 * 1000], async (err, stats) => {
                if (err) {
                    console.error("Database error in performance:", err);
                    return i.editReply({ content: "Database error occurred." });
                }
                
                const memoryUsage = process.memoryUsage();
                
                const embed = new EmbedBuilder()
                .setTitle(`${BOT_EMOJI} Bot Performance Metrics`)
                .setColor(0x00ff00)
                .addFields([
                    { name: "⚡ Uptime", value: formatDuration(client.uptime), inline: true },
                    { name: "🏠 Guilds", value: client.guilds.cache.size.toString(), inline: true },
                    { name: "📊 API Latency", value: `${client.ws.ping}ms`, inline: true },
                    { name: "💾 Memory Usage", value: `${(memoryUsage.heapUsed / 1024 / 1024).toFixed(2)} MB`, inline: true },
                    { name: "✅ Success Rate", value: `${performance.successes > 0 ? Math.round((performance.successes / (performance.successes + performance.errors)) * 100) : 100}%`, inline: true }
                ])
                .setFooter({ text: "ACS Performance Monitor" })
                .setTimestamp();
                
                // Add performance stats if available
                if (stats && stats.length > 0) {
                    let statsText = "";
                    stats.forEach(stat => {
                        statsText += `**${stat.action}**: ${Math.round(stat.avg_duration)}ms (${stat.count}x)\n`;
                    });
                    embed.addFields({ name: "📈 Recent Performance", value: statsText, inline: false });
                }
                
                embed.addFields({
                    name: "📊 Totals",
                    value: `Bans: ${performance.bansExecuted}\nUnbans: ${performance.unbansExecuted}\nStrikes: ${performance.strikesIssued}\nErrors: ${performance.errors}`,
                    inline: false
                });
                
                await i.editReply({ embeds: [embed] });
            });
        }
        
        // ================= EXPORT COMMAND =================
        if (i.commandName === "export") {
            await i.deferReply({ flags: 64 });
            
            const format = i.options.getString("format");
            
            try {
                const data = await exportBlacklist(format);
                const filename = `blacklist_export_${Date.now()}.${format}`;
                const buffer = Buffer.from(data, 'utf8');
                const attachment = new AttachmentBuilder(buffer, { name: filename });
                
                const embed = new EmbedBuilder()
                .setTitle(`${BOT_EMOJI} Export Complete`)
                .setColor(0x00ff00)
                .setDescription(`Exported blacklist data in ${format.toUpperCase()} format`)
                .addFields([
                    { name: "Filename", value: filename, inline: true },
                    { name: "Size", value: `${(buffer.length / 1024).toFixed(2)} KB`, inline: true }
                ])
                .setFooter({ text: "ACS Export System" })
                .setTimestamp();
                
                await i.editReply({ 
                    content: "Here's your export file:",
                    embeds: [embed],
                    files: [attachment],
                    flags: 64
                });
            } catch (error) {
                await i.editReply({ 
                    content: `❌ Export failed: ${error.message}`,
                    flags: 64 
                });
            }
        }
        
        // ================= IMPORT COMMAND =================
        if (i.commandName === "import") {
            await i.deferReply({ flags: 64 });
            
            const attachment = i.options.getAttachment("file");
            const filename = attachment.name.toLowerCase();
            
            // Determine format from filename
            let format;
            if (filename.endsWith('.json')) format = 'json';
            else if (filename.endsWith('.csv')) format = 'csv';
            else {
                return i.editReply({ content: "Unsupported file format. Please use JSON or CSV.", flags: 64 });
            }
            
            try {
                // Fetch the file
                const response = await fetch(attachment.url);
                const data = await response.text();
                
                // Import the data
                const result = await importBlacklist(data, format, i.user.id);
                
                const embed = new EmbedBuilder()
                .setTitle(`${BOT_EMOJI} Import Complete`)
                .setColor(0x00ff00)
                .setDescription(`Successfully imported blacklist data`)
                .addFields([
                    { name: "File", value: attachment.name, inline: true },
                    { name: "Format", value: format.toUpperCase(), inline: true },
                    { name: "Total Records", value: result.total.toString(), inline: true },
                    { name: "Valid Records", value: result.valid.toString(), inline: true },
                    { name: "Invalid Records", value: result.invalid.toString(), inline: true }
                ])
                .setFooter({ text: "ACS Import System" })
                .setTimestamp();
                
                await i.editReply({ embeds: [embed] });
                
                // Auto-enforce after import
                setTimeout(async () => {
                    await enforceBlacklist();
                }, 2000);
                
            } catch (error) {
                await i.editReply({ 
                    content: `❌ Import failed: ${error.message}`,
                    flags: 64 
                });
            }
        }
    }
    
    // ================= BUTTON INTERACTIONS FOR PAGINATION =================
    if (i.isButton()) {
        if (!isAuthorized(i.member)) {
            return i.reply({ content: "You don't have permission to use this.", flags: 64, ephemeral: true });
        }
        
        const [action, currentPageStr] = i.customId.split('_');
        const currentPage = parseInt(currentPageStr) || 1;
        
        if (i.customId === 'first_page') {
            await i.deferUpdate();
            await sendPaginatedBlacklist(i, 1);
        }
        else if (i.customId === 'prev_page') {
            await i.deferUpdate();
            // Get current page from the message footer
            const footerText = i.message.embeds[0]?.footer?.text || '';
            const match = footerText.match(/Page (\d+)\/(\d+)/);
            if (match) {
                const newPage = parseInt(match[1]) - 1;
                await sendPaginatedBlacklist(i, newPage);
            }
        }
        else if (i.customId === 'next_page') {
            await i.deferUpdate();
            // Get current page from the message footer
            const footerText = i.message.embeds[0]?.footer?.text || '';
            const match = footerText.match(/Page (\d+)\/(\d+)/);
            if (match) {
                const newPage = parseInt(match[1]) + 1;
                await sendPaginatedBlacklist(i, newPage);
            }
        }
        else if (i.customId === 'last_page') {
            await i.deferUpdate();
            // Get total pages from the message footer
            const footerText = i.message.embeds[0]?.footer?.text || '';
            const match = footerText.match(/Page \d+\/(\d+)/);
            if (match) {
                const totalPages = parseInt(match[1]);
                await sendPaginatedBlacklist(i, totalPages);
            }
        }
    }
});

// ================= GUILD CREATE (BOT JOINS SERVER) =================
client.on("guildCreate", async guild => {
    console.log(`Bot joined guild: ${guild.name} (${guild.id})`);
    
    // Check if server is blacklisted
    const isBlacklisted = await isServerBlacklisted(guild.id);
    
    if (isBlacklisted) {
        const reason = await getServerBlacklistReason(guild.id);
        console.log(`Server ${guild.name} is blacklisted (${reason}). Bot will stay but be COMPLETELY SILENT.`);
        // DO NOT LEAVE - just stay and do nothing
        return;
    }
});

// ================= UNBAN DETECTION =================
client.on("guildBanRemove", async ban => {
    console.log(`Ban remove detected in ${ban.guild.name} for user ${ban.user.id}`);
    
    // Check if guild is exempt
    if (config.exemptGuilds?.includes(ban.guild.id)) {
        console.log(`Guild ${ban.guild.name} is exempt, ignoring`);
        return;
    }
    
    // Check if server is blacklisted - if so, do ABSOLUTELY NOTHING - complete radio silence
    const isBlacklisted = await isServerBlacklisted(ban.guild.id);
    if (isBlacklisted) {
        console.log(`Guild ${ban.guild.name} is blacklisted - COMPLETE RADIO SILENCE - ignoring unban detection entirely`);
        return;
    }
    
    // Small delay to ensure audit log is updated
    await new Promise(r => setTimeout(r, 1000));
    
    db.get(`SELECT * FROM blacklist WHERE userId = ?`, [ban.user.id], async (err, row) => {
        if (err) {
            console.error("Database error checking blacklist:", err);
            return;
        }
        
        if (!row) {
            console.log(`User ${ban.user.id} is not blacklisted, ignoring`);
            return;
        }
        
        console.log(`Blacklisted user ${ban.user.id} was unbanned in ${ban.guild.name}`);
        
        // Get the unbanner information properly
        let unbannerId = null;
        let unbannerTag = "Unknown";
        let unbannerType = "user";
        
        try {
            const logs = await ban.guild.fetchAuditLogs({
                type: AuditLogEvent.MemberBanRemove,
                limit: 5
            });
            
            // Find the relevant audit log entry
            const entry = logs.entries.find(e => 
                e.target.id === ban.user.id && 
                Date.now() - e.createdTimestamp < 30000 // Within last 30 seconds
            );
            
            if (entry && entry.executor) {
                unbannerId = entry.executor.id;
                unbannerTag = entry.executor.tag;
                
                // Check if the executor is a bot
                if (entry.executor.bot) {
                    unbannerType = "bot";
                    console.log(`Unban executed by bot: ${unbannerTag}`);
                } else {
                    console.log(`Unban executed by user: ${unbannerTag}`);
                }
            } else {
                console.log(`Could not find unban executor in audit logs`);
            }
        } catch (error) {
            console.error("Failed to fetch audit logs:", error);
        }
        
        // Re-ban the user
        try {
            await banRateLimiter.execute(() => 
                ban.guild.members.ban(ban.user.id, { 
                    reason: `[ACS] Re-ban after unban attempt by ${unbannerTag}` 
                })
            );
            console.log(`Successfully re-banned ${ban.user.id} in ${ban.guild.name}`);
        } catch (error) {
            console.error(`Failed to re-ban ${ban.user.id} in ${ban.guild.name}:`, error);
        }
        
        logAction(ban.user.id, "UNBAN_ATTEMPT", 
                  `Server: ${ban.guild.name} (${ban.guild.id}) | By: ${unbannerTag} | Type: ${unbannerType}`);
        
        // Pass the unbanner info to addStrike
        try {
            await addStrike(ban.guild.id, ban.user.id, unbannerTag, unbannerId, unbannerType);
            console.log(`Strike added for guild ${ban.guild.id}`);
        } catch (strikeError) {
            console.error("Error adding strike:", strikeError);
        }
    });
});

// ================= READY =================
client.once("ready", async () => {
    console.log(`ACS Blacklist Enforcer online as ${client.user.tag}`);
    console.log(`Watching ${client.guilds.cache.size} guilds`);
    
    // Log blacklisted servers but DO NOT leave them
    const guilds = Array.from(client.guilds.cache.values());
    for (const guild of guilds) {
        const isBlacklisted = await isServerBlacklisted(guild.id);
        if (isBlacklisted) {
            console.log(`Currently in blacklisted guild ${guild.name} (${guild.id}) - Bot will be COMPLETELY SILENT here`);
        }
    }
    
    // Initial enforcement with delay
    setTimeout(async () => {
        const result = await enforceBlacklist();
        console.log(`Initial enforcement: ${result.banned.length} bans applied`);
    }, 5000);
    
    // Setup automatic backups daily
    setInterval(async () => {
        await backupDatabase();
    }, 24 * 60 * 60 * 1000);
});

client.login(config.token).catch(error => {
    console.error("Failed to login:", error);
    process.exit(1);
});
