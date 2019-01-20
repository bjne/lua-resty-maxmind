local ffi = require ('ffi')
local C = ffi.C
local ffi_str = ffi.string
local ffi_cast = ffi.cast

local _M = { _VERSION = 0.1 }
local mt = { __index = _M }

local shm_cache = ((ngx or {}).shared or {}).geoip

ffi.cdef[[
typedef unsigned int mmdb_uint128_t __attribute__ ((__mode__(TI)));

typedef struct MMDB_entry_s {
	struct MMDB_s *mmdb;
	uint32_t offset;
} MMDB_entry_s;

typedef struct MMDB_lookup_result_s {
	bool found_entry;
	MMDB_entry_s entry;
	uint16_t netmask;
} MMDB_lookup_result_s;

typedef struct MMDB_entry_data_s {
	bool has_data;

	union {
		uint32_t pointer;
		const char *utf8_string;
		double double_value;
		const uint8_t *bytes;
		uint16_t uint16;
		uint32_t uint32;
		int32_t int32;
		uint64_t uint64;
		mmdb_uint128_t uint128;
		bool boolean;
		float float_value;
	};

	uint32_t offset;
	uint32_t offset_to_next;
	uint32_t data_size;
	uint32_t type;
} MMDB_entry_data_s;

typedef struct MMDB_entry_data_list_s {
	MMDB_entry_data_s entry_data;
	struct MMDB_entry_data_list_s *next;
} MMDB_entry_data_list_s;

typedef struct MMDB_description_s {
	const char *language;
	const char *description;
} MMDB_description_s;

typedef struct MMDB_metadata_s {
	uint32_t node_count;
	uint16_t record_size;
	uint16_t ip_version;
	const char *database_type;
	struct {
		size_t count;
		const char **names;
	} languages;
	uint16_t binary_format_major_version;
	uint16_t binary_format_minor_version;
	uint64_t build_epoch;
	struct {
		size_t count;
		MMDB_description_s **descriptions;
	} description;
} MMDB_metadata_s;

typedef struct MMDB_ipv4_start_node_s {
	uint16_t netmask;
	uint32_t node_value;
} MMDB_ipv4_start_node_s;

typedef struct MMDB_s {
	uint32_t flags;
	const char *filename;
	ssize_t file_size;
	const uint8_t *file_content;
	const uint8_t *data_section;
	uint32_t data_section_size;
	const uint8_t *metadata_section;
	uint32_t metadata_section_size;
	uint16_t full_record_byte_size;
	uint16_t depth;
	MMDB_ipv4_start_node_s ipv4_start_node;
	MMDB_metadata_s metadata;
} MMDB_s;

typedef char * pchar;

MMDB_lookup_result_s MMDB_lookup_string(
	MMDB_s *const mmdb,
	const char *const ipstr,
	int *const gai_error,
	int *const mmdb_error);

int MMDB_open(const char *const filename, uint32_t flags, MMDB_s *const mmdb);
void MMDB_close(MMDB_s *const mmdb);

int MMDB_get_entry_data_list(
	MMDB_entry_s *start,
	MMDB_entry_data_list_s **const entry_data_list);

void MMDB_free_entry_data_list(MMDB_entry_data_list_s *const entry_data_list);

char *MMDB_strerror(int error_code);
const char *gai_strerror(int errcode);
]]

local MMDB_SUCCESS             = 0
local MMDB_OUT_OF_MEMORY_ERROR = 5
local MMDB_INVALID_DATA_ERROR  = 7

local EXTENDED, POINTER, UTF8_STRING, DOUBLE, BYTES, UINT16, UINT32, MAP,
      INT32, UINT64, UINT128, ARRAY, CONTAINER, END_MARKER, BOOLEAN, FLOAT
      = 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15

local ok, mm = pcall(ffi.load, 'maxminddb')
if not ok then
	mm = ffi.load('maxminddb.so.0')
end

local mmdb = ffi.typeof('MMDB_s')

local gai_error, mmdb_error = ffi.new('int[1]'), ffi.new('int[1]')

local function mmdb_strerror()
	return ffi_str(mm.MMDB_strerror(mmdb_error[0]))
end

local function gai_strerror()
	return ffi_str(C.gai_strerror(gai_error[0]))
end

local function dump_data_list(self, data_list)
	if not data_list then
		return nil, MMDB_INVALID_DATA_ERROR
	end

	local data_item = data_list[0].entry_data
	local data_type = data_item.type
	local data_size = data_item.data_size

	local result, status, key, value

	if data_type == MAP then
		result = {}

		data_list = data_list[0].next

		for key=1,data_size do
			data_item = data_list[0].entry_data

			if data_item.type ~= UTF8_STRING then
				return nil, MMDB_INVALID_DATA_ERROR
			end

			key = ffi_str(data_item.utf8_string, data_item.data_size)
			if not key then
				return nil, MMDB_OUT_OF_MEMORY_ERROR
			end

			data_list, status, value = dump_data_list(self, data_list[0].next)

			if status ~= MMDB_SUCCESS then
				return nil, status
			end

			if key ~= 'geoname_id' and key ~= self.skip_key then
				result[key] = value
			end
		end
	elseif data_type == ARRAY then
		result = {}

		for key=1,data_size do
			data_list, status, value = dump_data_list(self, data_list[0].next)

			if status ~= MMDB_SUCCESS then
				return nil, status
			end

			result[key] = value
		end
	else
		if data_type == UTF8_STRING then
			value = ffi_str(data_item.utf8_string, data_size)
		elseif data_type == BYTES then
			value = ffi_str(ffi_cast('char * ', data_item.bytes), data_size)
		elseif data_type == DOUBLE then
			value = data_item.double_value
		elseif data_type == FLOAT then
			value = data_item.float_value
		elseif data_type == UINT16 then
			value = data_item.uint16
		elseif data_type == UINT32 then
			value = data_item.uint32
		elseif data_type == BOOLEAN then
			value = data_item.boolean == 1
		elseif data_type == UINT64 then
			value = data_item.uint64
		elseif data_type == INT32 then
			value = data_item.int32
		else
			return nil, MMDB_INVALID_DATA_ERROR
		end

		if not value then
			return nil, MMDB_OUT_OF_MEMORY_ERROR
		end

		result = value
		data_list = data_list[0].next
	end

	return data_list, MMDB_SUCCESS, result
end

function _M.new(database_file, names)
	local db = ffi.gc(mmdb(), mm.MMDB_close)

	local ok = mm.MMDB_open(database_file, 0, db)
	if ok ~= MMDB_SUCCESS then
		return nil, 'failed to open file'
	end

	return setmetatable({ db = db, skip_key = names ~= false and 'names' }, mt)
end

function _M:lookup(ip)
	local result = mm.MMDB_lookup_string(self.db, ip, gai_error, mmdb_error)

	if mmdb_error[0] ~= MMDB_SUCCESS then
		return nil, 'lookup failed: ' .. mmdb_strerror()
	end

	if gai_error[0] ~= MMDB_SUCCESS then
		return nil, 'lookup failed: ' .. gai_strerror()
	end

	if result.found_entry ~= true then
		return nil, 'not found'
	end

	local entry_data_list = ffi_cast(
		'MMDB_entry_data_list_s **const', ffi.new("MMDB_entry_data_list_s")
	)

	local status = mm.MMDB_get_entry_data_list(result.entry, entry_data_list)

	if status ~= MMDB_SUCCESS then
		return nil, 'get entry data failed: ' .. mmdb_strerror(status)
	end

	local _, status, result = dump_data_list(self, entry_data_list)

	mm.MMDB_free_entry_data_list(entry_data_list[0])

	if status ~= MMDB_SUCCESS then
		return nil, 'dump entry data failed: ' .. mmdb_strerror(status)
	end

	return result
end

return _M
