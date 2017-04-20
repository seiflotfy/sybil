package sybil

/*
#cgo LDFLAGS: -lluajit-5.1
#include <stdlib.h>
#include <stdio.h>
#include <luajit-2.0/lua.h>
#include <luajit-2.0/lualib.h>
#include <luajit-2.0/lauxlib.h>
int init() {
return 0;
}

void set_intfield (lua_State *state, const char *index, int value) {
lua_pushstring(state, index);
lua_pushnumber(state, value);
lua_settable(state, -3);
}

void set_strfield (lua_State *state, const char *index, char *value) {
lua_pushstring(state, index);
lua_pushstring(state, value);
lua_settable(state, -3);
}

*/
import "C"

import (
	"fmt"
	"unsafe"
)

const src = `
local ffi = require('ffi')
ffi.cdef([[
    int init();
]])

function map(records)
  print("LUA CALLING MAP ON", #records, "RECORDS")
  for i=1,#records do
	local record = records[i]
  end

  return { count=#records }
end

function reduce(results, new)
  results.count = (results.count or 0) + (new.count or 10)
  return results

end

function finalize(results) 
  print("LUA FINALIZING RESULTS")
  results["finalized"] = 1
  return results
end

ffi.C.init()
  `

func (r *Record) toLuaTable(state *C.struct_lua_State) {
	if r == nil {
		return
	}

	C.lua_createtable(state, 0, 0)

	for name, val := range r.Ints {
		if r.Populated[name] == INT_VAL {
			col := r.block.GetColumnInfo(int16(name))
			fieldname := col.get_string_for_key(name)

			C.set_intfield(state, C.CString(fieldname), C.int(val))

		}
	}

	for name, val := range r.Strs {
		if r.Populated[name] == STR_VAL {
			col := r.block.GetColumnInfo(int16(name))
			strval := col.get_string_for_val(int32(val))
			fieldname := col.get_string_for_key(name)

			C.set_strfield(state, C.CString(fieldname), C.CString(strval))
		}
	}
}

func (rl *RecordList) toLuaTable(state *C.struct_lua_State) {
	C.lua_createtable(state, 0, 0)
	for i, r := range *rl {
		r.toLuaTable(state)
		C.lua_rawseti(state, -2, C.int(i+1))
	}

}

type LuaKey interface{}
type LuaTable map[string]interface{}

func setLuaTable(state *C.struct_lua_State, t LuaTable) {
	C.lua_createtable(state, 0, 0)

	// iterate through all keys in our table
	for k, v := range t {
		switch v := v.(type) {
		case bool:
		case int:
			C.set_intfield(state, C.CString(k), C.int(v))
		case string:
			C.set_strfield(state, C.CString(k), C.CString(v))
		case LuaTable:
			setLuaTable(state, v)
		default:
			fmt.Printf("unexpected type %T\n", v) // %T prints whatever type t has
		}
	}

}

func getLuaTable(state *C.struct_lua_State) LuaTable {
	/* table is in the stack at index 't' */
	it := C.int(C.lua_gettop(state))
	C.lua_pushnil(state) /* first key */

	ret := make(LuaTable, 0)

	for C.lua_next(state, it) != 0 {
		/* uses 'key' (at index -2) and 'value' (at index -1) */
		keytype := C.lua_type(state, -2)

		var key string
		var val LuaKey

		switch C.lua_type(state, -1) {
		case C.LUA_TNUMBER:
			val = int(C.lua_tonumber(state, -1))
		case C.LUA_TBOOLEAN:
			val = C.lua_toboolean(state, -1)
		case C.LUA_TSTRING:
			val = C.lua_tolstring(state, -1, nil)
		case C.LUA_TTABLE:
			val = getLuaTable(state)
		default:
			fmt.Printf("unexpected type %T\n", C.lua_type(state, -1)) // %T prints whatever type t has

		}

		if keytype == C.LUA_TSTRING {
			key = C.GoString(C.lua_tolstring(state, -2, nil))
		} else {
			key = fmt.Sprintf("%v", int(C.lua_tonumber(state, -2)))
		}

		ret[key] = val

		/* removes 'value'; keeps 'key' for next iteration */
		C.lua_settop(state, (-1)-1)
	}

	return ret

}

func (qs *QuerySpec) luaInit() {
	// Initialize state.
	qs.LuaState = C.luaL_newstate()
	state := qs.LuaState
	if state == nil {
		fmt.Println("Unable to initialize Lua context.")
	}
	C.luaL_openlibs(state)

	// Compile the script.
	csrc := C.CString(src)
	defer C.free(unsafe.Pointer(csrc))
	if C.luaL_loadstring(state, csrc) != 0 {
		errstring := C.GoString(C.lua_tolstring(state, -1, nil))
		fmt.Printf("Lua error: %v\n", errstring)
	}

	// Execute outer level
	if C.lua_pcall(state, 0, 0, 0) != 0 {
		errstring := C.GoString(C.lua_tolstring(state, -1, nil))
		fmt.Printf("Lua execution error: %v\n", errstring)
	}

}

func (qs *QuerySpec) luaMap(rl *RecordList) LuaTable {
	state := qs.LuaState
	// Execute map function
	C.lua_getfield(state, C.LUA_GLOBALSINDEX, C.CString("map"))
	rl.toLuaTable(state)
	if C.lua_pcall(state, 1, 1, 0) != 0 {
		errstring := C.GoString(C.lua_tolstring(state, -1, nil))
		fmt.Printf("Lua Reduce execution error: %v\n", errstring)
	} else {
		ret := getLuaTable(state)

		qs.LuaResult = ret
		return ret
	}
	return make(LuaTable, 0)

}

func (qs *QuerySpec) luaCombine(other *QuerySpec) LuaTable {
	// call to reduce
	state := qs.LuaState
	C.lua_getfield(state, C.LUA_GLOBALSINDEX, C.CString("reduce"))
	setLuaTable(state, qs.LuaResult)
	setLuaTable(state, other.LuaResult)

	if C.lua_pcall(state, 2, 1, 0) != 0 {
		errstring := C.GoString(C.lua_tolstring(state, -1, nil))
		fmt.Printf("Lua Combine execution error: %v\n", errstring)
	} else {

		combined := getLuaTable(state)

		qs.LuaResult = combined
		return combined
	}

	return make(LuaTable, 0)

}

func (qs *QuerySpec) luaFinalize() LuaTable {
	state := qs.LuaState
	// call to finalize
	C.lua_getfield(state, C.LUA_GLOBALSINDEX, C.CString("finalize"))
	setLuaTable(state, qs.LuaResult)

	if C.lua_pcall(state, 1, 1, 0) != 0 {
		errstring := C.GoString(C.lua_tolstring(state, -1, nil))
		fmt.Printf("Lua Finalize execution error: %v\n", errstring)
	} else {

		Debug("FINALIZING", qs.LuaResult)
		finalized := getLuaTable(state)
		qs.LuaResult = finalized

		Print("FINALIZED", qs.LuaResult)
		return finalized
	}

	return make(LuaTable, 0)
}
