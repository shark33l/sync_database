<h1>Sync Database <i style="font-weight:300">(Untitled for now)</i></h1>
<hr/>

Currently this is just a basic Database sync script.
This is developed to load **SnipeIT** `assets` table to **KEA DHCP** `hosts` table, but developed
in a generic way to easily modify to support other ETL tasks.
<i>Currently a Work in Progress.</i>

## Prequsites
`Python 3.x`

## Installation

1. Install [`virtualenv`](https://virtualenv.pypa.io/en/latest/user_guide.html) to work in a virtual environment and activate. *(optional, but recommended)*
```shell
cd sync_database
pip install virtualenv
```

2. Install dependecies related to this project using `requirements.txt`
```shell
pip install -r requirements.txt
```

3. Setup relevant configurations in [`main_config.json`](main_config.json)
4. Run the sync script
```shell
python sync_db.py
```

*TODO*

