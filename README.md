# cmi-pb-terminology

CMI-PB Controlled Terminology


## Server

We provide a simple Flask server that will serve terminology pages without
the need to build the ontology:

1. Clone the git repository:

```
cd /var/www
sudo git clone https://github.com/jamesaoverton/cmi-pb-terminology.git terminology
cd terminology
```

2. Set up a Python virtual environment:

```
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

3. Fetch a copy of the `cmi-pb.db`:

```
make fetch
```

4. Configure and start the `terminology.service`:

```
sudo ln -s /var/www/terminology/src/server/terminology.service /etc/systemd/system/terminology.service
sudo systemctl enable terminology.service
sudo systemctl start terminology.service
```
