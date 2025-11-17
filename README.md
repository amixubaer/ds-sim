### 1. Manual check (single config)

From `~/ds-sim`:

```bash
./ds-test/ds-server -c ds-test/TestConfigs/config10-long-med.xml -v brief -p 50000 -n
```
In another terminal:
```bash
cd ~/ds-sim
python3 client.py --algo ect --port 50000
```
### 2. Automated tests (all configs)

From `~/ds-sim/ds-test`:
```bash
python3 ds_test.py "python3 ../client.py --algo ect --port 50000" -n -p 50000
python3 mark_client.py
```
