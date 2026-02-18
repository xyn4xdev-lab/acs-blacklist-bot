# ACS Global Blacklist Bot

A Discord bot that manages global blacklists across multiple servers with automatic enforcement and strike tracking.

## Features

- Global blacklist management across all servers
- Server blacklisting (bot goes completely silent)
- Automatic banning of blacklisted users
- Unban detection with strike system
- Moderation logs and audit trails
- Export/import functionality (JSON/CSV)
- Performance monitoring
- Rate limiting protection
- Database backups

## Commands

### Blacklist Commands
| Command | Description | Options |
|---------|-------------|---------|
| `/blacklist add` | Blacklist a user globally | userid, reason |
| `/blacklist server` | Blacklist a server | guildid, reason |
| `/blacklist list` | Show all blacklisted users | page (optional) |
| `/blacklist serverlist` | Show all blacklisted servers | page (optional) |

### Unblacklist Commands
| Command | Description | Options |
|---------|-------------|---------|
| `/unblacklist user` | Remove a user from blacklist | userid, reason |
| `/unblacklist server` | Remove a server from blacklist | guildid, reason |

### Utility Commands
| Command | Description | Options |
|---------|-------------|---------|
| `/search` | Search for a user in blacklist | userid |
| `/modlogs` | View logs for a user | userid |
| `/strikes` | View strike information | guildid (optional) |
| `/reload` | Re-enforce blacklist | - |
| `/performance` | View bot metrics | - |
| `/export` | Export blacklist data | format |
| `/import` | Import blacklist data | file |

## Setup

1. **Clone the repository**
```bash
git clone git clone https://github.com/xyn4xdev-lab/acs-blacklist-bot.git
cd acs-blacklist-bot
```

2. **Install dependencies**
```bash
npm install discord.js sqlite3
```

3. **Create config.json**
```json
{
  "token": "bot token",
  "clientId": "bot client id",
  "controlGuildId": "guildid",
  "logGuildId": "guildid",
  "logChannelId": "channelid",
  "allowedRoles": ["roleid"],
  "appealServerLink": "https://discord.gg/YOUR_APPEAL_INVITE_CODE",
  "checkInterval": 300000,
  "notifyChannelId": "channelid"
}
```

4. **Run the bot**
```bash
node index.js
```

## Configuration Options

- `token` - Your bot token
- `clientId` - Bot client ID
- `controlGuildId` - Server where commands can be used
- `logGuildId` - Server for log channel
- `logChannelId` - Channel for global logs
- `allowedRoles` - Role IDs that can use commands
- `checkInterval` - Time in miliseconds that bot rechecks bans
- `appealServerLink` - Link for blacklisted users to appeal

## Database Structure

The bot uses SQLite with the following tables:
- `blacklist` - Blacklisted users
- `server_blacklist` - Blacklisted servers  
- `strikes` - Strike records per guild
- `strike_logs` - Detailed strike history
- `modlogs` - Moderation action logs
- `daily_stats` - Daily statistics
- `performance_stats` - Performance metrics
- `owner_dm_status` - Owner DM attempt tracking

## Strike System

- Unlimited strikes per server
- Each unban attempt adds a strike
- Notifications sent to:
  - User who performed the unban
  - Server owner
  - Global log channel
- Strike count displayed in notifications

## Server Blacklist

When a server is blacklisted:
- Bot stays in the server but goes completely silent
- No messages, logs, or enforcement actions
- No strikes are added
- Perfect for problematic servers

## Permissions Required

- `Ban Members` - To ban/unban users
- `View Audit Log` - To detect unbans
- `Send Messages` - For notifications
- `Read Message History` - For functionality

## Support

Join our support server for help: https://discord.gg/Hur9SgP5GZ

## License

This project is open source and available under the MIT License.
