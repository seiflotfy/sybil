//+build !luajit

package pkg

func initLua() {
	enableLua = false
}

type LuaKey interface{}
type LuaTable map[string]interface{}

func SetLuaScript(filename string) {}

func (qs *QuerySpec) luaInit() {}

func (qs *QuerySpec) luaMap(rl *recordList) {}

func (qs *QuerySpec) luaCombine(other *QuerySpec) {}

func (qs *QuerySpec) luaFinalize() {}
