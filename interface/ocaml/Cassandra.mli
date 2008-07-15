(*
 Autogenerated by Thrift

 DO NOT EDIT UNLESS YOU ARE SURE YOU KNOW WHAT YOU ARE DOING
*)

open Thrift
open Cassandra_types

class virtual iface :
object
  inherit FacebookService.iface
  method virtual get_slice : string option -> string option -> string option -> int option -> int option -> column_t list
  method virtual get_column : string option -> string option -> string option -> column_t
  method virtual get_column_count : string option -> string option -> string option -> int
  method virtual insert : string option -> string option -> string option -> string option -> int option -> unit
  method virtual batch_insert : batch_mutation_t option -> unit
  method virtual batch_insert_blocking : batch_mutation_t option -> bool
  method virtual remove : string option -> string option -> string option -> unit
  method virtual get_slice_super : string option -> string option -> string option -> int option -> int option -> superColumn_t list
  method virtual get_superColumn : string option -> string option -> string option -> superColumn_t
  method virtual batch_insert_superColumn : batch_mutation_super_t option -> unit
  method virtual batch_insert_superColumn_blocking : batch_mutation_super_t option -> bool
end

class client : Protocol.t -> Protocol.t -> 
object
  inherit FacebookService.client
  method get_slice : string -> string -> string -> int -> int -> column_t list
  method get_column : string -> string -> string -> column_t
  method get_column_count : string -> string -> string -> int
  method insert : string -> string -> string -> string -> int -> unit
  method batch_insert : batch_mutation_t -> unit
  method batch_insert_blocking : batch_mutation_t -> bool
  method remove : string -> string -> string -> unit
  method get_slice_super : string -> string -> string -> int -> int -> superColumn_t list
  method get_superColumn : string -> string -> string -> superColumn_t
  method batch_insert_superColumn : batch_mutation_super_t -> unit
  method batch_insert_superColumn_blocking : batch_mutation_super_t -> bool
end

class processor : iface ->
object
  inherit Processor.t

  inherit FacebookService.processor
  val processMap : (string, int * Protocol.t * Protocol.t -> unit) Hashtbl.t
  method process : Protocol.t -> Protocol.t -> bool
end

