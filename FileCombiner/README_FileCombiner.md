# FileCombiner
FileCombiner takes multiple files of the same type and appends to a larger file

## Use Case Description

Often we will receive several fragmented files and need to join them together. 
We can have >1000 csv files we need to join together. This problem becomes
magnified in ETL processes. For example BigQuery has a limit to how many writes 
can be made to a table partition, the maximum file size allowed to transfer, 
and charges for increasing disk space or quotas.  

This script is intended to be run from the command line. So you don't need to 
know python to utilize this script.

## Example Usage
Files are being dumped into a folder called raw_dump. They need to be combined
into 4 GB files, named liveramp and timestamped. The final file should be
placed into final_dump


```bash
python FileCombiner.py --sourceDir ./raw_dump --destinationDir ./final_dump --shardFileDir ./shards --fileExt gz --destinationFileName liveramp --sizeUnit GB --sizeLimit 4 --temporaryDir ./temp
```

- ./raw_dump (file A, file B) 
-- => ./temp (liveramp*datetime*)
-- move => ./shard (file A, file C)
- when file size up to 4GB
- ./temp (liveramp*datetime*) move ./final_dump (liveramp*datetime*) 

## Arguments

Additional arguments can be added to the command above with a -- following the argument name and a space. Order does not matter.

| Arguments | Description | Examples |
|---|---|---|
| sourceDir | Source location of files to combine | ./raw_dump |
| destinationDir | Location of the final combined file | ./final_dump |
| fileExt | This is the file type | csv, gz, or True to mean any type |
| destinationFileName | The name of the final combined file | liveramp or None (default) to mean the source file name |
| recurSearch | Look in subfolders | True to look in subfolders or False (default) to not look in subfolders |
| sizeLimit | The numerical size limit of the final file | 2 (default) or any natural number |
| sizeUnit | The unit of the size from sizeLimit | GB (default), MB, TB |
| timeName | If creation datetime should be in the name | True (default) to have datetime in the name, False if there is only one case so datetime is not necessary | 
| deleteFile | Delete the individual fragment files after combining |False (default) to not delete, True to delete|
| shardFileDir | If you don't delete the source files,they will be placed in the shardFile directory |False (default) will not move the files. It is ill advised to not delete the file and then not move it, '' will move it to ./processed, ./shards|
|temporaryDir|This creates a temporary directory so the combined file is not interruped while writing or forcibly removed|'' (default) creates and writes to ./temp, ./tempfolder|
