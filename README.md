# AppMetrica LogsAPI Loader

Python-script for automatic loading from AppMetrica LogsAPI into local ClickHouse DB. And yes, there is a Docker container too.

## How to use this image

To use this image you should generate [OAuth token][OAUTH-DOCS] for [AppMetrica Logs API][LOGSAPI-DOCS]. This token should be provided via environment variable `TOKEN`.

Also you should copy app's numeric IDs. You could find them in General Settings of your app ("Application ID"). All IDs should be provided as JSON-array via environment variable `APP_IDS`.

### Start manualy
```bash
docker run -d \
    --name clickhouse \
    yandex/clickhouse-server
docker run -d \
    --name appmetrica-logsapi-loader \
    --link clickhouse \
    --env 'CH_HOST=http://clickhouse:8123' \
    --env 'TOKEN=YOUR_OAUTH_TOKEN' \
    --env 'APP_IDS=["YOUR_APP_ID"]' \
    yandex/appmetrica-logsapi-loader
```

More information about [ClickHouse server image][CLICKHOUSE-SERVER].

### Start with Docker Compose
Download this repository *(or just `docker-compose.yml` file)* and run:
```bash
TOKEN=YOUR_OAUTH_TOKEN \
APP_IDS='["YOUR_APP_ID"]' \
docker-compose up -d
``` 

## Configuration

All configuration properties can be passed through environment variables.
 
#### Main variables
* `TOKEN` - *(required)* Logs API OAuth token.
* `APP_IDS` - *(required)* JSON-array of numeric AppMetrica app identifiers.
* `SOURCES` - Logs API endpoints to download from. See [available endpoints][LOGSAPI-ENDPOINTS].

#### ClickHouse related
* `CH_HOST` - Host of ClickHouse DB to store events. (default: `http://localhost:8123`)
* `CH_USER` - Login of ClickHouse DB. (default: empty)
* `CH_PASSWORD` - Password of ClickHouse DB. (default: empty)
* `CH_DATABASE` - Database in ClickHouse to create tables in. (default: `mobile`)

#### LogsAPI related
* `LOGS_API_HOST` - Base host of LogsAPI endpoints. (default: `https://api.appmetrica.yandex.ru`)
* `REQUEST_CHUNK_ROWS` - Size of chunks to process at once. (default: `25000`)
* `ALLOW_CACHED` - Flag that allows cached LogsAPI data. Possible values: `0`, `1`. (default: `0`)
* `PARTS_COUNT` - Start parts count for endpoint, should be valid JSON dictionary(`String:Int`). Possible values: `'{"events":10}'`. (default: `'{}'`)
   *Note: as described in LogsAPI documentation - for files more than 5GB server should return HTTP status `400` immediately, 
   but by some reason it starts to return data and reset connection in the middle. In such case loader should start with manually selected parts count.*

#### Scheduling configuration
* `UPDATE_LIMIT` - Count of days for the first events fetch. (default: `30`)
* `FRESH_LIMIT` - Count of days which still can have new events. (default: `7`)
* `UPDATE_INTERVAL` - Interval of time in hours between events fetches from Logs API. (default: `12`)

#### Other variables
* `DEBUG` - Enables extended logging. Possible values: `0`, `1`. (default: `0`)
* `STATE_FILE_PATH` - Path to file with script state. (default: `data/state.json`)

## License
License agreement on use of Yandex AppMetrica is available at [EULA site][LICENSE]


[LOGSAPI-DOCS]: https://tech.yandex.com/appmetrica/doc/mobile-api/logs/about-docpage/ "AppMetrica LogsAPI documentation"
[OAUTH-DOCS]: https://tech.yandex.com/appmetrica/doc/mobile-api/intro/authorization-docpage/ "Yandex OAuth documentation"
[CLICKHOUSE-SERVER]: https://hub.docker.com/r/yandex/clickhouse-server/ "ClickHouse Server Docker image page"
[LOGSAPI-ENDPOINTS]: https://tech.yandex.com/appmetrica/doc/mobile-api/logs/endpoints-docpage/ "AppMetrica LogsAPI Endpoints documentation"
[LICENSE]: https://yandex.com/legal/metrica_termsofuse/ "Yandex AppMetrica agreement"
