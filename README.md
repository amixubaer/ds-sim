### 1. Manual check (single config)

From `~/ds-sim`:

```bash
./ds-test/ds-server -c ds-test/TestConfigs/config10-long-med.xml -v brief -p 57922 -n
```
In another terminal:
```bash
cd ~/ds-sim
python3 client.py --algo ect --port 57922
```
### 2. Automated tests (all configs)

From `~/ds-sim/ds-test`:
```bash
python3 python3 ds_test.py "python3 ../client.py" -n -p 57922 -c TestConfigs
```
