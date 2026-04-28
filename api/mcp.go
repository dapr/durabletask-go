/*
Copyright 2026 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package api

import "github.com/dapr/durabletask-go/api/protos"

type InProcessWorkflow = protos.InProcessWorkflow
type MCPMethod = protos.MCPMethod

const (
	IN_PROCESS_WF_MCP InProcessWorkflow = protos.InProcessWorkflow_IN_PROCESS_WORKFLOW_MCP
)

const (
	MCP_METHOD_LIST_TOOLS MCPMethod = protos.MCPMethod_MCP_METHOD_LIST_TOOLS
	MCP_METHOD_CALL_TOOL  MCPMethod = protos.MCPMethod_MCP_METHOD_CALL_TOOL
)

// InProcessWorkflowPrefixes maps an InProcessWorkflow enum to its canonical
// string prefix used when constructing workflow names.
var InProcessWorkflowPrefixes = map[InProcessWorkflow]string{
	IN_PROCESS_WF_MCP: "dapr.internal.mcp.",
}

// MCPMethodSuffix maps an MCPMethod enum to its canonical method suffix
// used when constructing workflow names.
var MCPMethodSuffix = map[MCPMethod]string{
	MCP_METHOD_LIST_TOOLS: ".ListTools",
	MCP_METHOD_CALL_TOOL:  ".CallTool",
}

// MCPListToolsWorkflowName returns the full workflow name for a ListTools
// operation on the given MCPServer: dapr.internal.mcp.<server>.ListTools
func MCPListToolsWorkflowName(serverName string) string {
	return InProcessWorkflowPrefixes[IN_PROCESS_WF_MCP] + serverName + MCPMethodSuffix[MCP_METHOD_LIST_TOOLS]
}

// MCPCallToolWorkflowName returns the full workflow name for a CallTool
// operation on the given MCPServer: dapr.internal.mcp.<server>.CallTool
func MCPCallToolWorkflowName(serverName string) string {
	return InProcessWorkflowPrefixes[IN_PROCESS_WF_MCP] + serverName + MCPMethodSuffix[MCP_METHOD_CALL_TOOL]
}

// MCPListToolsActivityName returns the activity name for a ListTools transport
// call on the given MCPServer: dapr.internal.mcp.<server>.list-tools
func MCPListToolsActivityName(serverName string) string {
	return InProcessWorkflowPrefixes[IN_PROCESS_WF_MCP] + serverName + ".list-tools"
}

// MCPCallToolActivityName returns the activity name for a CallTool transport
// call on the given MCPServer: dapr.internal.mcp.<server>.call-tool
func MCPCallToolActivityName(serverName string) string {
	return InProcessWorkflowPrefixes[IN_PROCESS_WF_MCP] + serverName + ".call-tool"
}
