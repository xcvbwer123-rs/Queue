local module = require(game:GetService("ReplicatedStorage"):WaitForChild("Queue")) ---@module src
local Queue = module.new()

local function WaitAndPrint(AwesomeText: string)
	task.wait(5)
	print(AwesomeText)
end

for Index = 1, 5 do
	local Process = Queue:insert(WaitAndPrint, "Text"..("!"):rep(Index))

	if Index == 5 then
		Process:finally(function()
			print("Completed!")
		end)
	end
end