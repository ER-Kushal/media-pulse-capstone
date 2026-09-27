# Setting up a free Slack webhook (for automated alert notifications)

This takes about 2 minutes and is completely free.

1. Go to https://api.slack.com/apps and click **"Create New App"** → **"From scratch"**.
2. Name it something like `MediaPulse Alerts`, pick your own workspace (or
   create a free one at slack.com if you don't have one), click **Create App**.
3. In the left sidebar, click **"Incoming Webhooks"**, and toggle it **On**.
4. Click **"Add New Webhook to Workspace"**, pick a channel (e.g. `#alerts`),
   click **Allow**.
5. Copy the Webhook URL it gives you (starts with `https://hooks.slack.com/services/...`).
6. In Git Bash, before running the decision engine:
   ```
   export SLACK_WEBHOOK_URL="paste-your-url-here"
   python automation/decision_engine.py
   ```

If you skip this, the decision engine still runs and creates all the alert
rows in your database — it just prints `[notify skipped]` instead of
actually sending a Slack message. So it's optional for grading purposes,
but easy evidence to include in your demo video if you want it.
