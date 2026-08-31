# Launch media

`insidertrader-demo.gif` and `insidertrader-demo.mp4` are lightweight, credential-free
launch assets for the repository landing page. They are intentionally illustrative:
they communicate the workflow without pretending to show live performance or real
account data.

Before a public launch, replace them with a short paper-mode recording:

1. Run `./scripts/insider setup`, select paper/manual mode, and use a disposable CFG.
2. Start `./scripts/insider` and demonstrate `CHART AAPL`, `STRAT`, `METRICS`, and `RISK`.
3. Do not show API keys, account IDs, journal paths containing secrets, or live orders.
4. Capture the terminal/browser window with a screen recorder and export a 10–20 second
   GIF plus an MP4. Keep the same filenames so the README does not need editing.

To regenerate the illustrative assets locally:

```bash
./scripts/build-demo-media.sh
```
