local new_tab = require "table.new"
local ffi = require "ffi"

local cast = ffi.cast

local float_t = ffi.typeof('float *')
local double_t = ffi.typeof('double *')

local floor = math.floor
local ceil = math.ceil
local find = string.find
local byte = string.byte
local sub = string.sub
local rev = string.reverse

local band = bit.band
local lshift = bit.lshift
local rshift = bit.rshift

local EXTENDED, POINTER, UTF8_STRING, DOUBLE, BYTES, UINT16, UINT32, MAP,
      INT32, UINT64, UINT128, ARRAY, CONTAINER, END_MARKER, BOOLEAN, FLOAT
      = 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15

--    TODO: INT32 UINT128 CONTAINER

local _M = { _VERSION = 0.1 }

function _M:byte(pos, len)
	return byte(self.mmdb, pos, pos + (len or 1) - 1)
end

local function read_data(self, pos)
	local ctrl_byte, pos, mmdb = self:byte(pos), pos + 1, self.mmdb
	local data_type, data_size = band(rshift(ctrl_byte, 5), 0x7), ctrl_byte % 32

	if data_type == EXTENDED then
		data_type, pos = self:byte(pos) + 7, pos + 1
	end

	if data_size == 29 then
		data_size, pos = 29 + self:byte(pos), pos + 1
	elseif data_size == 30 then
		local a, b = self:byte(pos, 2)
		data_size, pos = 285 + a * 256 + b, pos + 2
	elseif data_size == 31 then
		local a, b, c, d = self:byte(pos, 4)
		data_size, pos = 65821 + a * 16777216 + b * 65536 + c * 256 + d, pos + 4
	end

	if data_type == UINT16 or data_type == UINT32 or data_type == UINT64 then
		local n = 0
		for i=data_size,1,-1 do
			n = n + (2^((i-1)*8) * self:byte(pos + data_size - i))
		end

		return pos + data_size, n
	end

	if data_type == POINTER then
		local size, ptr = floor(data_size/8)

		if size > 3 then
		elseif size == 0 then
			local a = self:byte(pos)
			ptr, pos = (data_size % 8) * 256 + a, pos + 1
		elseif size == 1 then
			local a, b = self:byte(pos, 2)
			ptr, pos = (data_size % 8) * 65536 + a * 256 + b + 2048, pos + 2
		elseif size == 2 then
			local a, b, c = self:byte(pos, 3)
			ptr = (data_size % 8) * 16777216 + a * 65536 + b * 256 + c + 526336
			pos = pos + 3
		elseif size == 3 then
			local a, b, c, d = self:byte(pos, 4)
			ptr, pos = a * 16777216 + b * 65536 + c * 256 + d, pos + 4
		end

		return pos, select(2, read_data(self, self.start_data + ptr))
	end

	if data_type == UTF8_STRING or data_type == BYTES then
		return pos + data_size, sub(mmdb, pos, pos + data_size - 1)
	end

	if data_type == MAP then
		local t, k = new_tab(0, data_size)
		for i=1,data_size do
			pos, k = read_data(self, pos)
			pos, t[k] = read_data(self, pos)
		end

		return pos, t
	end

	if data_type == ARRAY then
		local t = new_tab(data_size, 0)
		for i=1,data_size do
			pos, t[i] = read_data(self, pos)
		end

		return pos, t
	end

	if data_type == BOOLEAN then
		return pos, data_size == 1
	end

	if data_type == FLOAT then
		local data = rev(sub(mmdb, pos, pos + data_size - 1))
		return pos + data_size, cast(float_t, data)[0]
	end

	if data_type == DOUBLE then
		local data = rev(sub(mmdb, pos, pos + data_size - 1))
		return pos + data_size, cast(double_t, data)[0]
	end

	if data_type == END_MARKER then
		return nil
	end

	return nil, string.format("unsupported data type: %d", data_type)
end

local function read_metadata(self)
	local e, p = -(1024 * 128)
	repeat
		e = select(2, find(self.mmdb, "\171\205\239MaxMind.com", e + 1, true))
		p = e or p
	until e == nil

	return p and select(2, read_data(self, p + 1))
end

function _M:lookup(node, ...)
	local left, right = self.left, self.right
	local record_length, node_count = self.record_length, self.node_count

	for i=1,select('#', ...) do
		local n, x, v = (select(i, ...))
		for b=7,0,-1 do
			x = rshift(band(n, lshift(1, b)), b)
			v = (x == 1 and right or left)(self, node * record_length + 1)

			if v == node_count then
				return nil
			end

			if v > node_count then
				local offset = self.start_data + v - node_count - 16
				return select(2, read_data(self, offset))
			end

			node = v
		end
	end
end

_M.new = function(mmdb_filename)
	local file, err = io.open(mmdb_filename, "rb")
	if not file then
		return nil, err
	end

	local mmdb = file:read('*all')

	local self = setmetatable({ mmdb = mmdb }, { __index = _M })

	local metadata = read_metadata(self)

	if not metadata then
		return nil, "failed to locate metadata block"
	end

	-- TODO: support multiple record_lengths
	local left = function(self, pos)
		local a, b, c = self:byte(pos, 3)
		return a * 65536 + b * 256 + c
	end

	local right = function(self, pos)
		local a, b, c = self:byte(pos + 3, 3)
		return a * 65536 + b * 256 + c
	end

	self.right = right
	self.left = left
	self.node_count = metadata.node_count
	self.record_length = ceil(metadata.record_size * 2 / 8)
	self.start_data = self.record_length * self.node_count + 16 + 1

	return self
end

return _M
