# Batch & near real-time data platform

> For deployment steps follow Deployment.md and for details about the project raed Report.md

> Below is the project delivery directory structure. 

```
.
├── LICENSE
├── README.md
├── code
│   ├── README.md
│   └── mysimbdp
│       ├── batch-app
│       │   ├── app.py
│       │   ├── manager.py
│       │   ├── tenant-1-const.yaml
│       │   ├── tenant-1.py
│       │   ├── tenant-2-const.yaml
│       │   └── tenant-2.py
│       ├── docker-compose.yaml
│       ├── mongo
│       │   ├── config.init.js
│       │   ├── enable-shard.js
│       │   ├── router.init.js
│       │   ├── shardrs.init.js
│       │   └── user.init.js
│       ├── mysimbdp-up.sh
│       ├── requirements.txt
│       └── stream-app
│           ├── app.py
│           ├── manager.py
│           ├── tenant-1-model.yaml
│           ├── tenant-1.py
│           ├── tenant-2-model.yaml
│           └── tenant-2.py
├── data
│   └── client-staging-input-directory
│       ├── processed
│       │   └── reviews.csv
│       ├── reviews.csv
│       └── ruuvitag.csv
├── logs
│   ├── batch_log_batch-topic-1.csv
│   ├── batch_log_batch-topic-2.csv
│   ├── stream_log_stream-topic-1.csv
│   └── stream_log_stream-topic-2.csv
└── reports
    ├── deployment.md
    ├── report.md
    └── resources
        ├── 1-data-wrangling.png
        ├── 2-data-wrangling.png
        ├── Screenshot 2024-03-14 at 12.59.38 PM.png
        ├── batch-log.png
        ├── confluent-up.png
        ├── data-dictionary.png
        ├── dynamic-allocation.png
        ├── kafka-UI.png
        ├── kafka-topics.png
        ├── log-monitor.png
        ├── mysimbdp.png
        ├── overview.png
        ├── part-1-arch.png
        ├── part-2-arch.png
        ├── processed.png
        ├── resources-overview.png
        ├── spark-UI.png
        ├── spark-up.png
        ├── stream-1-const.png
        ├── stream-2-const.png
        ├── stream-ingest.png
        ├── stream-log.png
        ├── stream-log2.png
        ├── stream-toic-msg.png
        └── violation.png
```