--// Variables
local module = {}
local weakV = {__mode = "v"}
local queues = setmetatable({}, weakV)

local Process = {}
local Waitter = {}
local RateQueue = {}

local __callnew = {}
local void = (function()end)()

--// Types
type fn<T...> = (T...) -> ...any;

type constructor = {
	new: (id: string?) -> Queue;
	queues: {[string]: Queue};
	prototype: QueueImpl&{[string]: fn<(Queue, ...any)>};

	Process: processContsructor;
	Waitter: waitterConstructor;
	RateQueue: rateQueueConstructor;
}

export type Queue = {
	id: string;
	queues: {Process};
} & QueueImpl

type QueueImpl = {
	insert: <T...>(self: Queue, handler: fn<T...>, T...) -> Process;
	insertFront: <T...>(self: Queue, handler: fn<T...>, T...) -> Process;

	remove: (self: Queue, process: Process) -> boolean;
	removeById: (self: Queue, processId: string) -> boolean;

	findProcessById: (self: Queue, id: string) -> Process?;
}

type rateQueueConstructor = {new: (ratePerMinute: number, id: string?) -> RateQueue, findById: (id: string) -> RateQueue?, rateQueues: {[string]: RateQueue}, prototype: RateQueueImpl&{[string]: fn<(RateQueue, ...any)>}} & ((ratePerMinute: number, id: string?) -> RateQueue)

export type RateQueue = {
	id: string;
	ratePerMinute: number;
	queues: {Process};
} & RateQueueImpl

type RateQueueImpl = {
	insert: <T...>(self: Queue, handler: fn<T...>, T...) -> Process;
	insertFront: <T...>(self: Queue, handler: fn<T...>, T...) -> Process;

	remove: (self: Queue, process: Process) -> boolean;
	removeById: (self: Queue, processId: string) -> boolean;

	findProcessById: (self: Queue, id: string) -> Process?;
}

type waitterConstructor = {new: (id: string?) -> Waitter, prototype: WaitterImpl&{[string]: fn<(Waitter, ...any)>}, waitters: {[string]: Waitter}} & ((id: string?) -> Waitter)

export type Waitter = {
	id: string;
	container: {Process};
} & WaitterImpl

type WaitterImpl = {
	insert: (self: Waitter, process: Process) -> Process;
	remove: (self: Waitter, process: Process) -> Process?;
	removeById: (self: Waitter, id: string) -> Process?;
	destroy: (self: Waitter) -> ();
	await: (self: Waitter) -> Waitter;
	executeAll: (self: Waitter) -> Waitter;
}

type processContsructor = {new: <T...>(handler: fn<T...>, T...) -> Process, prototype: ProcessImpl&{[string]: fn<(Process, ...any)>}} & (<T...>(handler: fn<T...>, T...) -> Process)

type ProcessImpl = {
	await: (self: Process) -> Process;
	andThen: <T...>(self: Process, handler: fn<T...>, T...) -> Process;
	catch: <T...>(self: Process, handler: fn<T...>, T...) -> Process;
	finally: <T...>(self: Process, handler: fn<T...>, T...) -> Process;
	execute: (self: Process) -> Process;

	hasError: (self: Process) -> false | string;
	getResults: (self: Process) -> ...any;
	getResultTable: (self: Process) -> {any};
}

export type Process = {
	id: string;
	handler: fn<...any>;
	arguments: {any};
	status: "pending" | "running" | "completed";
	success: boolean?;
} & ProcessImpl

--// Functions
local function randomString(origin: string?)
	local random = math.random(0, 15)
	local value = origin == 'x' and random or (bit32.band(random, 0x3) + 8)
	return string.format('%x', value)
end

local function getId(id: string?)
	if not id then
		id = ("xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx"):gsub("[xy]", randomString):upper()
	end

	return id
end

local function safeLen(t: {any})
	local a=0;for _,_ in pairs(t)do a=a+1 end;return a
end

function __callnew:__call(...)
	return self.new(...)
end

--// Classes
-- | Process
do
	local processImpl = {}

	Process.prototype = processImpl

	local function resolve(process, pipeline)
		local handler, args, status = table.unpack(pipeline)

		if status == nil or process.__results[1] == status then
			local results

			if safeLen(args) > 0 then
				results = {pcall(handler, table.unpack(args), table.unpack(process.__results[2] or {}))}
			else
				results = {pcall(handler, table.unpack(process.__results[2] or {}))}
			end

			process.__results[1] = table.remove(results, 1)

			if #results > 0 then
				process.__results[2] = results
			end
		end
	end

	local function executeProcess(process)
		process.status = "running"

		local results = {pcall(process.handler, table.unpack(process.arguments))}
		local success = table.remove(results, 1)

		process.__results[1] = success
		process.__results[2] = results

		if process.__fastAwaits then
			while #process.__fastAwaits > 0 do
				task.defer(table.remove(process.__fastAwaits, 1))
			end
		end

		while #process.__pipelines > 0 do
			local pipeline = table.remove(process.__pipelines, 1)

			resolve(process, pipeline)
		end

		process.status = "completed"
		process.__pipelines = void

		if process.__awaits then
			while #process.__awaits > 0 do
				task.defer(table.remove(process.__awaits, 1))
			end
		end

		process.__awaits = void
	end

	function Process:__index(key: string)
		if key == "success" then
			return self.__results[1] == true
		else
			return rawget(processImpl, key)
		end
	end

	function Process.new<T...>(handler: fn<T...>, ...: T...): Process
		return setmetatable({
			id = getId();
			handler = handler;
			arguments = {...};
			status = "pending";
			success = false;
			__pipelines = {};
			__results = {};
		}, Process)
	end

	function processImpl:await(fastMode: boolean?)
		if self.status ~= "completed" then
			local index = fastMode and "__fastAwaits" or "__awaits"
			self[index] = self[index] or {}
			table.insert(self[index], coroutine.running())
			coroutine.yield()
		end

		return self
	end

	function processImpl:hasError()
		if self.status ~= "completed" then
			self:await()
		end

		if not self.__results[1] then
			return self.__results[2][1]
		end

		return false
	end

	function processImpl:getResults()
		if self.status ~= "completed" then
			self:await()
		end

		local errorMsg = self:hasError()

		if errorMsg then
			return error(errorMsg, 2)
		else
			return table.unpack(self.__results[2])
		end
	end

	function processImpl:getResultTable()
		if self.status ~= "completed" then
			self:await()
		end
		
		local errorMsg = self:hasError()

		if errorMsg then
			return error(errorMsg, 2)
		else
			return self.__results[2]
		end
	end

	function processImpl:andThen(handler, ...)
		if self.status ~= "completed" then
			table.insert(self.__pipelines, {handler, {...}, true})
		else
			task.defer(resolve, self, {handler, {...}, true})
		end

		return self
	end

	function processImpl:catch(handler, ...)
		if self.status ~= "completed" then
			table.insert(self.__pipelines, {handler, {...}, false})
		else
			task.defer(resolve, self, {handler, {...}, false})
		end

		return self
	end

	function processImpl:finally(handler, ...)
		if self.status ~= "completed" then
			table.insert(self.__pipelines, {handler, {...}})
		else
			task.defer(resolve, self, {handler, {...}})
		end

		return self
	end

	function processImpl:execute()
		if self.status == "pending" then
			self.__execution = task.defer(executeProcess, self)
		end

		return self
	end

	setmetatable(Process, __callnew)
end

-- | Queue
do
	local queueImpl = {}

	module.__index = queueImpl
	module.prototype = queueImpl

	function module.new(id: string?)
		if not queues[id] then
			queues[id] = setmetatable({
				id = getId(id);
				queues = {};
			}, module)
		end

		return queues[id]
	end

	function queueImpl:_activate(force: boolean?)
		if not force and self._active then return end
		self._active = true

		local process = table.remove(self.queues, 1)

		if process then
			process:execute():await(true):andThen(self._activate, self, true)
		else
			self._active = void
		end
	end

	function queueImpl:_insert<T...>(front: boolean, handler: fn<T...>, ...: T...): Process
		local Process = Process.new(handler, ...)

		if front then
			table.insert(self.queues, 1, Process)
		else
			table.insert(self.queues, Process)
		end

		queueImpl:_activate()

		return Process
	end

	function queueImpl:removeById(processId: string)
		for index, process in ipairs(self.queues) do
			if process.id == processId then
				table.remove(self.queues, index)
				return true
			end
		end

		return false
	end

	function queueImpl:remove(process: Process)
		return self:removeById(process.id)
	end

	function queueImpl:insert(...): Process
		return self:_insert(false, ...)
	end

	function queueImpl:insertFront(...): Process
		return self:_insert(true, ...)
	end

	function queueImpl:findProcessById(processId: string)
		for _, process in ipairs(self.queues) do
			if process.id == processId then
				return process
			end
		end
	end
end

-- | Waitter
do
	local waitterImpl = {}
	local waitters = setmetatable({}, weakV)

	Waitter.__index = waitterImpl
	Waitter.prototype = waitterImpl
	Waitter.waitters = waitters

	function Waitter.new(id: string?)
		if not waitters[id] then
			waitters[id] = setmetatable({
				id = getId(id);
				container = {};
				__threads = {};
			}, Waitter)
		end

		return waitters[id]
	end

	function waitterImpl:await()
		if #self.container > 0 then
			table.insert(self.__threads, coroutine.running())
			coroutine.yield()
		end

		return self
	end

	function waitterImpl:executeAll()
		for _, process in ipairs(self.container) do
			process:execute()
		end

		return self
	end

	function waitterImpl:removeById(processId: string)
		for index, process in ipairs(self.container) do
			if process.id == processId then
				table.remove(self.container, index)
				return process
			end
		end
	end

	function waitterImpl:remove(process: Process)
		return self:removeById(process.id)
	end

	function waitterImpl:destroy()
        waitters[self.id] = nil
        
        while #self.__threads > 0 do
            task.defer(table.remove(self.__threads, 1))
        end

        setmetatable(self, nil)
        table.clear(self.container)
        table.clear(self.__threads)
        table.clear(self)
    end

	function waitterImpl:insert(process: Process)
		if process.status ~= "completed" then
			table.insert(self.container, process)

			process:finally(function(...)
				local index = table.find(self.container, process)

				if index then
					table.remove(self.container, index)
				end

				if #self.container <= 0 then
					while #self.__threads > 0 do
						task.defer(table.remove(self.__threads, 1))
					end
				end

				return ...
			end)
		end

		return process
	end

	waitterImpl.Destroy = waitterImpl.destroy
	setmetatable(Waitter, __callnew)
end

-- | RateQueue
do
	local rateQueueImpl = setmetatable({}, module)
	local rateQueues = setmetatable({}, weakV)

	RateQueue.__index = rateQueueImpl
	RateQueue.prototype = rateQueueImpl
	RateQueue.rateQueues = rateQueues

	function RateQueue.new(ratePerMinute: number, id: string?)
		if not rateQueues[id] then
			rateQueues[id] = setmetatable({
				id = getId(id);
				queues = {};
				ratePerMinute = ratePerMinute;
				__lastActivated = tick();
			}, RateQueue)
		end

		return rateQueues[id]
	end

	function RateQueue.findById(id: string)
		return rateQueues[id]
	end

	function rateQueueImpl:_activate(isInternal: boolean?)
		if isInternal then
			local timeGone = tick() - self.__lastActivated
			local rate = self.ratePerMinute/60

			if timeGone < (1 / rate) then
				self.__process = task.delay((1 / rate) - timeGone, self._activate, self, true)
				return
			end
		end

		local process = table.remove(self.queues, 1)

		if process then
			process:execute()

			self.__lastActivated = tick()
			self.__process = task.delay((60 / self.ratePerMinute), self._activate, self, true)
		else
			self.__process = void
		end
	end

	function rateQueueImpl:_insert<T...>(front: boolean, handler: fn<T...>, ...: T...): Process
		local Process = Process.new(handler, ...)

		if front then
			table.insert(self.queues, 1, Process)
		else
			table.insert(self.queues, Process)
		end

		if not self.__process then
			self:_activate(true)
		end

		return Process
	end

	setmetatable(RateQueue, __callnew)
end

--// Set Properties
module.Process = Process
module.Waitter = Waitter
module.RateQueue = RateQueue
module.queues = queues

return table.freeze(module) :: constructor