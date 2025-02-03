# NYU-Advanced-Database-Systems

Replicated Concurrency Control and Recovery (RepCRec)

## Team members:

Zihan Zhang, Net ID: zz4412

Sathvika Bhagyalakshmi Mahesh, Net ID: sb8913

## How to run this program

You should use the following command to run the program

```python
python3 main.py [input_dictionary_path]
```

- 1. The `input_dictionary_path` is provided (note its a folder path). `input_dictionary_path` is the path where your test cases are, it will run all test cases under this path, and print all results.
- 2. The `input_dictionary_path` is NOT provided. It will automatically run the default test cases provided by use under `Tests` folder.

## How to reprozip and reprounzip

```python
reprozip trace python3 main.py "<Testfolder>"
reprozip pack [Pack_Name]
```

then

```python
reprounzip directory setup [Pack_Name].rpz ./unzip
reprounzip directory run ./unzip
```

## Tests folder

passed all these of files inside
