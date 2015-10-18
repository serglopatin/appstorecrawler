# appstorecrawler

This is appstore crawler script, that scrapes all apps and their icons from appstore website using multiple threads and socks proxies.

To use socks proxy, global variable *m_socks_proxy_base_port* must be set to non-zero value. For example, if base socks port is 8080, first thread will connect via 8080 port, second thread - 8081, third thread - 8082 port and so on.

By default all results will be saved to "results.csv" file. There is also "profiles_done.db" file - sqlite database, which contains indexed table with processed urls.

*m_num_workers* - this variable indicates number of threads, 1 thread by default.

*m_retries_num* - number of tries to fetch page content.

*m_failed_urls_filename* - text file with failed urls.

## How to run

To run script, install requirements into your virtualenv and then run script as:

`python appstore.py`

or to run in background:

`nohup python appstore.py >/dev/null 2>&1 &`
