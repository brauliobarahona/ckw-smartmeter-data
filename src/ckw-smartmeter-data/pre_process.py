import os
import glob
import pyarrow as pa
import pyarrow.csv
import pyarrow.parquet as pq
import pyarrow.dataset as ds

def extract_ids(ds_fn, _dir):
  # read parquet dataset and parse ids
  #print(f_out)
  print(" ... ")
  f_id = os.path.join(_dir, "IDS_" + os.path.basename(ds_fn))

  if os.path.exists(f_id):
    print("File {:} is already created: ".format(f_id))
    ids_arr = pa.array(pq.ParquetDataset(f_id).read()[0])

  elif not os.path.exists(f_id):
    print(".")
    ids = pq.ParquetDataset(ds_fn).read(columns=["id"]) # *!* 11 GB for a 20 GB file!?
    ids_arr = pa.array(pa.compute.unique(ids[0]))
    
    # save these ids to a file 
    print("{:} unique smart meter ids".format(len(ids_arr))) 
    pq.write_table(pa.table([ids_arr], names=["id"]), f_id)

  # TODO: write out file with unique ids of whole dataset
  return ids_arr

def get_uniqueIDs(l_ids, f_save=None, md_str=None):
  ids = pa.compute.unique(pa.concat_arrays(l_ids))
  T_id = pa.Table.from_pydict({"id": ids}, 
    metadata=md_str)
  
  if f_save:
    pq.write_table(T_id, os.path.join(ddir, f_save))

  """
  pa.Table.from_arrays([ids], names=["ids"]).schema # .from_batches()
                                            # .from_pandas()
                                            # .from_pydict()
                                            # .from_pydict(pydict, metadata=my_md)
                                            #    where my_md={"col name": "description"}
  """
  
  return ids, T_id
# ------------------------------------------ #
# pre-process to generate time series per id #
# ------------------------------------------ #
flg_extract = False
ddir = "/home/ubuntu/data/ckw/storage"
fns = glob.glob(ddir+"/*")
fns.sort()

f_ids = [i for i in fns if os.path.basename(i).startswith("IDS")]
f_d = [i for i in fns if os.path.basename(i).startswith("ckw") &  
                          os.path.basename(i).endswith(".parquet")]
f_uniqueID = [i for i in fns if os.path.basename(i).startswith("all_")][0]
# "all_IDS_ckw_dataset_a.parquet"

if flg_extract:
  list_ids = []
  for fi in f_d:
    print(fi)
    list_ids.append(extract_ids(fi, ddir))

  #md_str = {"id": "unique ids found in datasets from Jan 2021 to Jan-Feb 2024"}
  ids, _ = get_uniqueIDs(list_ids) # returns array, and table

elif not flg_extract and f_uniqueID:
  # read from file
  T_id = pq.read_table(f_uniqueID)

# ---------------------- #
# create datasets per ID #
# ---------------------- #
out_dir = "/home/ubuntu/data/ckw/ts/batch_0424"
glob.os.makedirs(out_dir, exist_ok=True)

# *!* check if there are any files already there and select different IDs if sort
fns = glob.glob(out_dir + "/*.parquet") # 10.03.24 17 hrs :: 896 files parsed so far
                                        # 23.04.24 18 hrs :: 5000 files parsed so far

for i in T_id[0][5000:20000]:
  print(". . . {:}".format(i))
  ts_per_id = []
  for fi in f_d: # source datasets
    print(fi)
    dset = ds.dataset(fi) # pyarrow._dataset.FileSystemDataset
  
    # filter rows while scanning
    _scanner = dset.scanner(columns=['timestamp', 'value_kwh'],
                      filter=ds.field("id") == i)
    _df = _scanner.to_table().sort_by("timestamp").replace_schema_metadata()

    ts = pa.compute.cast(_df["timestamp"], pa.timestamp("ms", tz="utc"))
    _df = pa.Table.from_pydict(
            {"timestamp": ts, 
            "kwh": _df["value_kwh"]}
            ).replace_schema_metadata(metadata = 
            {"kwh":"Consumption in kWh in ID:" + str(i)}
            )
    # retrive ID with pa.compute.cast(_df.schema.metadata[b"kwh"][-32:], pa.string())
    ts_per_id.append(_df)
  # write out time series of each id
  ts = pa.concat_tables(ts_per_id)
  pq.write_table(ts, os.path.join(out_dir, str(i) + ".parquet" ))

# --------------------------------------------- #
# batch create table of kWh with ids as columns #
# --------------------------------------------- #
# TODO: this approach should be much more efficient;
#           need to split the 107 k IDs into batches,
#           10? 100? batches
#
""" testing
  #dset = pq.ParquetDataset(fns[1]) # pyarrow.parquet.core.ParquetDataset
  _df = dset.head(3000).sort_by(["id", "timestamp"])

  expr = pa.compute.field("timestamp") <= "2021-01-16T07:00:00.000Z"
  _df.filter(expr)

  expr = pa.compute.field("id") == "03a25f298431549a6bc0b1a58eca1f34"
  _df.filter(expr).sort_by("timestamp")
"""
""" testing polars
(
    pl.scan_pyarrow_dataset(dset)
    .filter("bools")
    .select("bools", "floats", "date")
    .collect()
)
"""
""" *!* test gzip
with gzip.open(fn, "rb") as f:
  file_content = f.read()
  f_out
  with open("storage/{}".format(fn.replace(".gz", "")), "wb") as f:
    f.write(file_content)¨
    print("Unzipped file: {}".format(fn))
"""