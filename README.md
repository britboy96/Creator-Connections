# Creators Connections — TikTok → Discord Bot

**What it does**
- Tracks your TikTok LIVE gifts & likes by just your `@username` (no webhooks).
- Posts a single **Creators Connections** image after every LIVE with:
  - Left: **Top Gifters** (top 10)
  - Right: **Top Tappers** (likes) (top 10)
- Weekly summary (Sat 19:00 GMT/UTC) with same image + rotates the **Sore Finger** role to weekly top tapper.
- After each LIVE, rotates **Top Gifter** role.
- Auto-creates roles, DMs new members to link (`/tokconnect`), includes keep-alive web server.

## Quick start

1) **Put your background image** at: `assets/creators_connections_bg.png`  
2) Create `.env` from `.env.example` and set your **DISCORD_BOT_TOKEN**.
3) Install and run:
```bash
pip install -r requirements.txt
python bot.py
```

### Discord setup
- Invite the bot with permissions: **Send Messages**, **Attach Files**, **Read Message History**, **Manage Roles**.
- Ensure the bot's role is **above** `Top Gifter` and `Sore Finger` in Role hierarchy.

### Slash commands
- `/toktrack <tiktok_name>` (admin) — set the **host** TikTok account to track.
- `/set_target_channel #channel` — set where images post.
- `/start_tiktok` / `/stop_tiktok` — begin/end tracking.
- `/tokconnect <tiktok_name>` — users link their TikTok to their Discord for name display and roles.
- `/post_connect_prompt` (admin) — post & pin the CTA to link accounts.
- `/backscan [limit] [channel]` (admin) — scan recent messages to auto-link handles like `@name` or `tiktok.com/@name`.

### Weekly schedule
- Defaults to **Saturday 19:00 UTC**.  
- To change: adjust guild timezone and schedule in DB (extend with commands if desired).

### Render deploy
- Create a **Web Service** on Render.
- Build: `pip install -r requirements.txt`
- Start: `python bot.py`
- Add env vars from `.env.example`.
- Add an UptimeRobot **HTTP(s) monitor** to `https://<your-app>.onrender.com/health`.

