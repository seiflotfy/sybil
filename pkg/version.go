package pkg

var VERSION_STRING = "0.2.0"

func GetVersionInfo() map[string]interface{} {
	version_info := make(map[string]interface{})

	version_info["version"] = VERSION_STRING
	version_info["FieldSeparator"] = true
	version_info["LogHist"] = true
	version_info["query_cache"] = true

	if ENABLE_HDR {
		version_info["HdrHist"] = true

	}

	return version_info

}
