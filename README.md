# Sqoop Extractor for iasWorld

This repository contains the dependencies and scripts necessary to run [`sqoop`](https://sqoop.apache.org/docs/1.4.7/SqoopUserGuide.html), a data extraction tool for transferring data from relational databases to Hadoop, Hive, or Parquet.

In this case, `sqoop` is used to export table dumps from iasWorld, the CCAO's system of record, to an [HCatalog](https://cwiki.apache.org/confluence/display/Hive/HCatalog). The result is a set of partitioned and bucketed Parquet files which can be uploaded to [AWS S3](https://aws.amazon.com/s3/) and queried directly via [AWS Athena](https://aws.amazon.com/athena).

## Structure

### Directories

- `docker-config/` - Configuration and setup files for the Hadoop/Hive backend. Used during Docker build only
- `drivers/` - Mounted during run to provide connection drivers to `sqoop`. Put OJDBC files here (`ojdbc8.jar` or `ojdbc7.jar`)
- `logs/` - Location of temporary log files. Logs are manually uploaded to AWS CloudWatch after each run is complete
- `scripts/` - Runtime scripts to run `sqoop` jobs within Docker
- `secrets/` - Mounted during run to provide DB password via a file. Alter `secrets/IPTS_PASSWORD` to contain your password
- `tables/` - Table definitions and metadata used to create Hive tables for `sqoop` to extract to. Manually stored since certain tables include partitioning and bucketing
- `target/` - Mounted during run as output directory. All parquet files and job artifacts are saved temporarily before being uploaded to S3

### Important Files

- `Dockerfile` - Dockerfile to build `sqoop` and all dependencies from scratch if unavailable via the GitLab container registry
- `run.sh` - Main entrypoint script. Idempotent. Run with `sudo ./run.sh` to extract all iasWorld tables.
- `docker-compose.yaml` - Defines the containers and environment needed to run `sqoop` jobs in a small, distributed Hadoop/Hive environment
- `.env` - Contains DB connection details. Alter before running to provide your own details

## Usage

### Dependencies

You will need the following tools installed before using this repo:

- [Docker](https://docs.docker.com/get-docker/)
- [Docker Compose](https://docs.docker.com/compose/install/)
- [AWS CLI v2](https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html) - Authenticated using `aws configure`
- [moreutils](http://joeyh.name/code/moreutils/) - For the `ts` timestamp command
- [jq](https://stedolan.github.io/jq/) - To parse logs to JSON

The rest of the dependencies for `sqoop` are installed using the included `Dockerfile`. To retrieve them, run either of the following commands within the repo directory:

- `docker-compose pull` - Grabs the latest image from the CCAO GitHub registry, if it exists
- `docker-compose build` - Builds the `sqoop` image from the included `Dockerfile`

### Update table schemas

If tables schemas are altered in iasWorld (column type change, new columns), then the associated table schema files need to be updated in order to extract the altered tables from iasWorld. To update the schema files:

1. (Optional) If new tables have been added, they must be added to `tables/tables-list.csv`
2. Change `/tmp/scripts/run-sqoop.sh` to `/tmp/scripts/get-tables.sh` in `docker-compose.yaml`
3. Run `docker compose up` and wait for the schema files (`tables/$TABLE.sql`) to update
4. Run `./update-tables.sh` to add bucketing and partitioning to the table schemas
5. Update the cron job in the README with any new tables, as well as the actual cronjob using `sudo crontab -e`

### Export Tables

Nearly all the functionality of this repository is contained in `run.sh`. This script will complete four main tasks:

1. Extract the specified tables from iasWorld and save them to the `target/` directory as Parquet
2. Remove any existing files on S3 for the extracted tables
3. Upload the extracted Parquet files to S3
4. Upload a logstream of the extraction and uploading process to CloudWatch

By default, `sudo ./run.sh` will export _all_ tables in iasWorld to `target/` (and then to S3). To extract a specific table or tables, prefix the run command with the environmental variable `IPTS_TABLE`. For example `sudo IPTS_TABLE="ASMT_HIST CNAME" ./run.sh` will grab the `ASMT_HIST` and `CNAME` tables

You can also specify a `TAXYR` within `IPTS_TABLE` using conditional symbols. For example, `sudo IPTS_TABLE="ASMT_HIST>2019 ADDRINDX=2020" ./run.sh` will get _only_ records with a `TAXYR` greater than 2019 for `ASMT_HIST` and only records with a `TAXYR` equal to 2020 for `ADDRINDX`. Only `=`, `<`, and `>` are allowed as conditional operators.

## Scheduling

Table extractions are schedule via [`cron`](https://man7.org/linux/man-pages/man8/cron.8.html). To edit the schedule file, use `sudo crontab -e`. The example below schedules daily jobs for frequently updated tables and weekly ones for rarely-updated tables.

```bash
# Extract recent years from frequently used tables on weekdays at 1 AM CST
0 6 * * 1,2,3,4,5 cd /local/path/to/repo && YEAR="$(($(date '+\%Y') - 2))" IPTS_TABLE="ADDN>$YEAR APRVAL>$YEAR ASMT_HIST>$YEAR ASMT_ALL>$YEAR COMDAT>$YEAR CVLEG>$YEAR DWELDAT>$YEAR ENTER HTPAR>$YEAR LEGDAT>$YEAR OBY>$YEAR OWNDAT>$YEAR PARDAT>$YEAR PERMIT SALES SPLCOM>$YEAR" /bin/bash ./run.sh

# Extract all tables except for ASMT_ALL and ASMT_HIST on Saturday at 1 AM CST
0 6 * * 6 cd /local/path/to/repo && IPTS_TABLE="AASYSJUR ADDN ADDRINDX APRVAL CNAME COMDAT COMFEAT COMINTEXT COMNT COMNT3 CVLEG CVOWN CVTRAN DEDIT DWELDAT ENTER EXADMN EXAPP EXCODE EXDET HTAGNT HTDATES HTPAR LAND LEGDAT LPMOD LPNBHD OBY OWNDAT PARDAT RCOBY PERMIT SALES SPLCOM VALCLASS" /bin/bash ./run.sh
```

## Useful Resources

### Documentation and Guides

- [Sqoop User Guide v1.4.7](https://sqoop.apache.org/docs/1.4.7/SqoopUserGuide.html)
- [Hadoop Cluster Setup Guide](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/ClusterSetup.html)
- [How to install and setup a 3-node Hadoop cluster](https://www.linode.com/docs/guides/how-to-install-and-set-up-hadoop-cluster/)
- [Setup a distributed Hadoop cluster with Docker](https://blog.newnius.com/setup-distributed-hadoop-cluster-with-docker-step-by-step.html)
- [Tips for Docker/Hadoop cluster setup](https://medium.com/@rubenafo/some-tips-to-run-a-multi-node-hadoop-in-docker-9c7012dd4e26)

### Docker Resources

- [Sqoop Docker image](https://github.com/dvoros/docker-sqoop)
- [Hadoop Docker image](https://github.com/dvoros/hadoop-docker) - Sqoop image uses this as a dependency
- [Hadoop cluster Docker image](https://github.com/rancavil/hadoop-single-node-cluster) - Useful setup and options for pseudo-distributed Hadoop

### Hive/HCatalog Resources

- [Docker Hadoop Setup](https://github.com/big-data-europe/docker-hadoop)
- [HCatalog Setup](https://www.tutorialspoint.com/hcatalog/hcatalog_installation.htm)
- [Hive Metastore Setup Reference](https://github.com/big-data-europe/docker-hive-metastore-postgresql)

### Debugging

- [SO post on `--bindir` sqoop option](https://stackoverflow.com/questions/21599785/sqoop-not-able-to-import-table)
- [SO post on `--map-column-java` option](https://stackoverflow.com/questions/32537148/sqoop-export-from-hdfs-to-oracle-error)
- [On generating strings for `--map-column-java` using shell and awk](https://stackoverflow.com/questions/45052340/how-to-use-sqoop-import-command-with-map-column-hive/45053915#45053915)
- [Post on java security options that cause `Connection reset` errors](https://blog.pythian.com/connection-resets-when-importing-from-oracle-with-sqoop/)
- [Data nodes not connected/listed by Hadoop](https://stackoverflow.com/questions/29910805/namenode-datanode-not-list-by-using-jps)
- [Oracle JDBC connection issues](https://stackoverflow.com/questions/2327220/oracle-jdbc-intermittent-connection-issue)

### Data Type Mappings (used to populate `--map-column-hive`)

- [HCatalog Data Types](https://cwiki.apache.org/confluence/display/Hive/HCatalog+InputOutput#HCatalogInputOutput-HCatRecord)
- [Java to Oracle Type Mapping Matrix](https://docs.oracle.com/cd/E19501-01/819-3659/gcmaz/)
- [PL/SQL (Oracle SQL) to JDBC mapping](https://docs.oracle.com/cd/B19306_01/java.102/b14188/datamap.htm#CHDBJAGH) - Not applicable here but still helpful
