# The VLSC Document Hub
Base path: C:\\Users\\mchri\\OneDrive - The VLSC\\The_VLSC_DocumentHub
This hub is processed by a local Python worker and routed by Power Automate.
Only **01_Processing\\Staging** is watched by Power Automate.

## Local Power Automate emulation
The Python worker now also produces `.eml` files in `Logs/Outbox` so a local
Power Automate flow or the Books account can pick up and forward staged
documents without needing cloud access.
