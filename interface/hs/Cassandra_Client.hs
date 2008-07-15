module Cassandra_Client(get_slice,get_column,get_column_count,insert,batch_insert,batch_insert_blocking,remove,get_slice_super,get_superColumn,batch_insert_superColumn,batch_insert_superColumn_blocking) where
import FacebookService_Client
import Data.IORef
import Thrift
import Data.Generics
import Control.Exception
import qualified Data.Map as Map
import qualified Data.Set as Set
import Data.Int
import Cassandra_Types
import Cassandra
seqid = newIORef 0
get_slice (ip,op) arg_tablename arg_key arg_columnFamily_column arg_start arg_count = do
  send_get_slice op arg_tablename arg_key arg_columnFamily_column arg_start arg_count
  recv_get_slice ip
send_get_slice op arg_tablename arg_key arg_columnFamily_column arg_start arg_count = do
  seq <- seqid
  seqn <- readIORef seq
  writeMessageBegin op ("get_slice", M_CALL, seqn)
  write_Get_slice_args op (Get_slice_args{f_Get_slice_args_tablename=Just arg_tablename,f_Get_slice_args_key=Just arg_key,f_Get_slice_args_columnFamily_column=Just arg_columnFamily_column,f_Get_slice_args_start=Just arg_start,f_Get_slice_args_count=Just arg_count})
  writeMessageEnd op
  tflush (getTransport op)
recv_get_slice ip = do
  (fname, mtype, rseqid) <- readMessageBegin ip
  if mtype == M_EXCEPTION then do
    x <- readAppExn ip
    readMessageEnd ip
    throwDyn x
    else return ()
  res <- read_Get_slice_result ip
  readMessageEnd ip
  case f_Get_slice_result_success res of
    Just v -> return v
    Nothing -> do
      throwDyn (AppExn AE_MISSING_RESULT "get_slice failed: unknown result")
get_column (ip,op) arg_tablename arg_key arg_columnFamily_column = do
  send_get_column op arg_tablename arg_key arg_columnFamily_column
  recv_get_column ip
send_get_column op arg_tablename arg_key arg_columnFamily_column = do
  seq <- seqid
  seqn <- readIORef seq
  writeMessageBegin op ("get_column", M_CALL, seqn)
  write_Get_column_args op (Get_column_args{f_Get_column_args_tablename=Just arg_tablename,f_Get_column_args_key=Just arg_key,f_Get_column_args_columnFamily_column=Just arg_columnFamily_column})
  writeMessageEnd op
  tflush (getTransport op)
recv_get_column ip = do
  (fname, mtype, rseqid) <- readMessageBegin ip
  if mtype == M_EXCEPTION then do
    x <- readAppExn ip
    readMessageEnd ip
    throwDyn x
    else return ()
  res <- read_Get_column_result ip
  readMessageEnd ip
  case f_Get_column_result_success res of
    Just v -> return v
    Nothing -> do
      throwDyn (AppExn AE_MISSING_RESULT "get_column failed: unknown result")
get_column_count (ip,op) arg_tablename arg_key arg_columnFamily_column = do
  send_get_column_count op arg_tablename arg_key arg_columnFamily_column
  recv_get_column_count ip
send_get_column_count op arg_tablename arg_key arg_columnFamily_column = do
  seq <- seqid
  seqn <- readIORef seq
  writeMessageBegin op ("get_column_count", M_CALL, seqn)
  write_Get_column_count_args op (Get_column_count_args{f_Get_column_count_args_tablename=Just arg_tablename,f_Get_column_count_args_key=Just arg_key,f_Get_column_count_args_columnFamily_column=Just arg_columnFamily_column})
  writeMessageEnd op
  tflush (getTransport op)
recv_get_column_count ip = do
  (fname, mtype, rseqid) <- readMessageBegin ip
  if mtype == M_EXCEPTION then do
    x <- readAppExn ip
    readMessageEnd ip
    throwDyn x
    else return ()
  res <- read_Get_column_count_result ip
  readMessageEnd ip
  case f_Get_column_count_result_success res of
    Just v -> return v
    Nothing -> do
      throwDyn (AppExn AE_MISSING_RESULT "get_column_count failed: unknown result")
insert (ip,op) arg_tablename arg_key arg_columnFamily_column arg_cellData arg_timestamp = do
  send_insert op arg_tablename arg_key arg_columnFamily_column arg_cellData arg_timestamp
send_insert op arg_tablename arg_key arg_columnFamily_column arg_cellData arg_timestamp = do
  seq <- seqid
  seqn <- readIORef seq
  writeMessageBegin op ("insert", M_CALL, seqn)
  write_Insert_args op (Insert_args{f_Insert_args_tablename=Just arg_tablename,f_Insert_args_key=Just arg_key,f_Insert_args_columnFamily_column=Just arg_columnFamily_column,f_Insert_args_cellData=Just arg_cellData,f_Insert_args_timestamp=Just arg_timestamp})
  writeMessageEnd op
  tflush (getTransport op)
batch_insert (ip,op) arg_batchMutation = do
  send_batch_insert op arg_batchMutation
send_batch_insert op arg_batchMutation = do
  seq <- seqid
  seqn <- readIORef seq
  writeMessageBegin op ("batch_insert", M_CALL, seqn)
  write_Batch_insert_args op (Batch_insert_args{f_Batch_insert_args_batchMutation=Just arg_batchMutation})
  writeMessageEnd op
  tflush (getTransport op)
batch_insert_blocking (ip,op) arg_batchMutation = do
  send_batch_insert_blocking op arg_batchMutation
  recv_batch_insert_blocking ip
send_batch_insert_blocking op arg_batchMutation = do
  seq <- seqid
  seqn <- readIORef seq
  writeMessageBegin op ("batch_insert_blocking", M_CALL, seqn)
  write_Batch_insert_blocking_args op (Batch_insert_blocking_args{f_Batch_insert_blocking_args_batchMutation=Just arg_batchMutation})
  writeMessageEnd op
  tflush (getTransport op)
recv_batch_insert_blocking ip = do
  (fname, mtype, rseqid) <- readMessageBegin ip
  if mtype == M_EXCEPTION then do
    x <- readAppExn ip
    readMessageEnd ip
    throwDyn x
    else return ()
  res <- read_Batch_insert_blocking_result ip
  readMessageEnd ip
  case f_Batch_insert_blocking_result_success res of
    Just v -> return v
    Nothing -> do
      throwDyn (AppExn AE_MISSING_RESULT "batch_insert_blocking failed: unknown result")
remove (ip,op) arg_tablename arg_key arg_columnFamily_column = do
  send_remove op arg_tablename arg_key arg_columnFamily_column
send_remove op arg_tablename arg_key arg_columnFamily_column = do
  seq <- seqid
  seqn <- readIORef seq
  writeMessageBegin op ("remove", M_CALL, seqn)
  write_Remove_args op (Remove_args{f_Remove_args_tablename=Just arg_tablename,f_Remove_args_key=Just arg_key,f_Remove_args_columnFamily_column=Just arg_columnFamily_column})
  writeMessageEnd op
  tflush (getTransport op)
get_slice_super (ip,op) arg_tablename arg_key arg_columnFamily_superColumnName arg_start arg_count = do
  send_get_slice_super op arg_tablename arg_key arg_columnFamily_superColumnName arg_start arg_count
  recv_get_slice_super ip
send_get_slice_super op arg_tablename arg_key arg_columnFamily_superColumnName arg_start arg_count = do
  seq <- seqid
  seqn <- readIORef seq
  writeMessageBegin op ("get_slice_super", M_CALL, seqn)
  write_Get_slice_super_args op (Get_slice_super_args{f_Get_slice_super_args_tablename=Just arg_tablename,f_Get_slice_super_args_key=Just arg_key,f_Get_slice_super_args_columnFamily_superColumnName=Just arg_columnFamily_superColumnName,f_Get_slice_super_args_start=Just arg_start,f_Get_slice_super_args_count=Just arg_count})
  writeMessageEnd op
  tflush (getTransport op)
recv_get_slice_super ip = do
  (fname, mtype, rseqid) <- readMessageBegin ip
  if mtype == M_EXCEPTION then do
    x <- readAppExn ip
    readMessageEnd ip
    throwDyn x
    else return ()
  res <- read_Get_slice_super_result ip
  readMessageEnd ip
  case f_Get_slice_super_result_success res of
    Just v -> return v
    Nothing -> do
      throwDyn (AppExn AE_MISSING_RESULT "get_slice_super failed: unknown result")
get_superColumn (ip,op) arg_tablename arg_key arg_columnFamily = do
  send_get_superColumn op arg_tablename arg_key arg_columnFamily
  recv_get_superColumn ip
send_get_superColumn op arg_tablename arg_key arg_columnFamily = do
  seq <- seqid
  seqn <- readIORef seq
  writeMessageBegin op ("get_superColumn", M_CALL, seqn)
  write_Get_superColumn_args op (Get_superColumn_args{f_Get_superColumn_args_tablename=Just arg_tablename,f_Get_superColumn_args_key=Just arg_key,f_Get_superColumn_args_columnFamily=Just arg_columnFamily})
  writeMessageEnd op
  tflush (getTransport op)
recv_get_superColumn ip = do
  (fname, mtype, rseqid) <- readMessageBegin ip
  if mtype == M_EXCEPTION then do
    x <- readAppExn ip
    readMessageEnd ip
    throwDyn x
    else return ()
  res <- read_Get_superColumn_result ip
  readMessageEnd ip
  case f_Get_superColumn_result_success res of
    Just v -> return v
    Nothing -> do
      throwDyn (AppExn AE_MISSING_RESULT "get_superColumn failed: unknown result")
batch_insert_superColumn (ip,op) arg_batchMutationSuper = do
  send_batch_insert_superColumn op arg_batchMutationSuper
send_batch_insert_superColumn op arg_batchMutationSuper = do
  seq <- seqid
  seqn <- readIORef seq
  writeMessageBegin op ("batch_insert_superColumn", M_CALL, seqn)
  write_Batch_insert_superColumn_args op (Batch_insert_superColumn_args{f_Batch_insert_superColumn_args_batchMutationSuper=Just arg_batchMutationSuper})
  writeMessageEnd op
  tflush (getTransport op)
batch_insert_superColumn_blocking (ip,op) arg_batchMutationSuper = do
  send_batch_insert_superColumn_blocking op arg_batchMutationSuper
  recv_batch_insert_superColumn_blocking ip
send_batch_insert_superColumn_blocking op arg_batchMutationSuper = do
  seq <- seqid
  seqn <- readIORef seq
  writeMessageBegin op ("batch_insert_superColumn_blocking", M_CALL, seqn)
  write_Batch_insert_superColumn_blocking_args op (Batch_insert_superColumn_blocking_args{f_Batch_insert_superColumn_blocking_args_batchMutationSuper=Just arg_batchMutationSuper})
  writeMessageEnd op
  tflush (getTransport op)
recv_batch_insert_superColumn_blocking ip = do
  (fname, mtype, rseqid) <- readMessageBegin ip
  if mtype == M_EXCEPTION then do
    x <- readAppExn ip
    readMessageEnd ip
    throwDyn x
    else return ()
  res <- read_Batch_insert_superColumn_blocking_result ip
  readMessageEnd ip
  case f_Batch_insert_superColumn_blocking_result_success res of
    Just v -> return v
    Nothing -> do
      throwDyn (AppExn AE_MISSING_RESULT "batch_insert_superColumn_blocking failed: unknown result")
