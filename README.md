# 📡 ISP Receipt Generator (Multi-Threaded Network Auditor)

A headless, Python-based dual-stack network auditing tool designed to bypass single-thread QoS limits, map Geo-DNS edge nodes, and measure true physical line capacity.

Most standard web-based speed tests fall victim to "Happy Eyeballs" protocol interference, geographic DNS traps, or single-connection throttling. This script was built to rip through those limitations using concurrent socket streams to aggregate bandwidth and mathematically verify an ISP's BGP routing tables.

## ✨ Key Features

* **Multi-Threaded Payload Engine:** Bypasses ISP per-connection QoS limits by opening parallel HTTP/TCP streams (e.g., 4 streams) to max out the physical fiber line's true throughput.
* **Dual-Stack Independent Testing:** Fully isolates IPv4 and IPv6 traffic. Prevents the OS from failing over to IPv4 when testing next-generation network performance.
* **Geo-DNS Edge Mapping:** Uses dynamic socket resolution right before ping sweeps to verify if the ISP's DNS is actually routing traffic to the closest local CDN cache or dropping it onto terrible international transit routes.
* **BGP "Blackhole" Detection:** Maps broken ISP routing by cross-referencing successful DNS resolutions with ICMP packet drops, proving exactly which subnets the ISP has failed to configure.
* **Automated CSV Receipts:** Generates timestamped forensic logs (`isp_receipts.csv`) containing Latency, Jitter, Throughput, Loaded Latency (Bufferbloat), and Target IP Resolutions.

## 🛠️ Prerequisites

This script requires Python 3.8+ and the following libraries:

```bash
pip install pandas requests


🚀 Usage 
Run the script directly from your terminal. Bash python NetworkTestv2.py
The script will prompt you for the desired number of parallel download/upload streams. For standard fibre lines (100-500 Mbps), 2 to 4 streams are recommended to bypass basic traffic shaping.

⚠️ Disclaimer: This tool initiates highly aggressive, concurrent data streams to test physical line capacity. Running this continuously or with an excessive number of threads may trigger your ISP's DDoS protection or violate Fair Usage Policies (FUP). Use responsibly for personal network auditing.
