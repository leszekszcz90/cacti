"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || function (mod) {
    if (mod && mod.__esModule) return mod;
    var result = {};
    if (mod != null) for (var k in mod) if (k !== "default" && Object.prototype.hasOwnProperty.call(mod, k)) __createBinding(result, mod, k);
    __setModuleDefault(result, mod);
    return result;
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.PluginLedgerConnectorBesu = exports.E_KEYCHAIN_NOT_FOUND = void 0;
const run_time_error_cjs_1 = require("run-time-error-cjs");
const rxjs_1 = require("rxjs");
const typescript_optional_1 = require("typescript-optional");
const viem_1 = require("viem");
const web3_1 = __importDefault(require("web3"));
const web3js_quorum_1 = __importDefault(require("web3js-quorum"));
const cactus_core_api_1 = require("@hyperledger/cactus-core-api");
const cactus_core_1 = require("@hyperledger/cactus-core");
const cactus_common_1 = require("@hyperledger/cactus-common");
const deploy_contract_solidity_bytecode_endpoint_1 = require("./web-services/deploy-contract-solidity-bytecode-endpoint");
const deploy_contract_solidity_bytecode_no_keychain_endpoint_1 = require("./web-services/deploy-contract-solidity-bytecode-no-keychain-endpoint");
const index_1 = require("./generated/openapi/typescript-axios/index");
const typescript_axios_1 = require("./generated/openapi/typescript-axios");
const invoke_contract_endpoint_1 = require("./web-services/invoke-contract-endpoint");
const model_type_guards_1 = require("./model-type-guards");
const sign_transaction_endpoint_v1_1 = require("./web-services/sign-transaction-endpoint-v1");
const prometheus_exporter_1 = require("./prometheus-exporter/prometheus-exporter");
const get_prometheus_exporter_metrics_endpoint_v1_1 = require("./web-services/get-prometheus-exporter-metrics-endpoint-v1");
const watch_blocks_v1_endpoint_1 = require("./web-services/watch-blocks-v1-endpoint");
const get_balance_endpoint_1 = require("./web-services/get-balance-endpoint");
const get_transaction_endpoint_1 = require("./web-services/get-transaction-endpoint");
const get_past_logs_endpoint_1 = require("./web-services/get-past-logs-endpoint");
const run_transaction_endpoint_1 = require("./web-services/run-transaction-endpoint");
const get_block_v1_endpoint_1 = require("./web-services/get-block-v1-endpoint-");
const get_besu_record_endpoint_v1_1 = require("./web-services/get-besu-record-endpoint-v1");
const get_open_api_spec_v1_endpoint_1 = require("./web-services/get-open-api-spec-v1-endpoint");
const grpc_default_service = __importStar(require("./generated/proto/protoc-gen-ts/services/default_service"));
const besu_grpc_svc_streams = __importStar(require("./generated/proto/protoc-gen-ts/services/besu-grpc-svc-streams"));
const besu_grpc_svc_open_api_1 = require("./grpc-services/besu-grpc-svc-open-api");
const besu_grpc_svc_streams_1 = require("./grpc-services/besu-grpc-svc-streams");
const get_block_v1_http_1 = require("./impl/get-block-v1/get-block-v1-http");
const transact_v1_impl_1 = require("./impl/transact-v1/transact-v1-impl");
const deploy_contract_v1_keychain_1 = require("./impl/deploy-contract-v1/deploy-contract-v1-keychain");
const deploy_contract_v1_no_keychain_1 = require("./impl/deploy-contract-v1/deploy-contract-v1-no-keychain");
const watch_events_v1_endpoint_1 = require("./web-services/watch-events-v1-endpoint");
const openapi_json_1 = __importDefault(require("../json/openapi.json"));
exports.E_KEYCHAIN_NOT_FOUND = "cactus.connector.besu.keychain_not_found";
class PluginLedgerConnectorBesu {
    options;
    instanceId;
    prometheusExporter;
    log;
    logLevel;
    web3Provider;
    web3;
    viemClient;
    viemTransport;
    viemWebSocketTransportConfig;
    web3Quorum;
    pluginRegistry;
    contracts = {};
    endpoints;
    txSubject = new rxjs_1.ReplaySubject();
    static CLASS_NAME = "PluginLedgerConnectorBesu";
    get className() {
        return PluginLedgerConnectorBesu.CLASS_NAME;
    }
    constructor(options) {
        this.options = options;
        const fnTag = `${this.className}#constructor()`;
        cactus_common_1.Checks.truthy(options, `${fnTag} arg options`);
        cactus_common_1.Checks.truthy(options.rpcApiHttpHost, `${fnTag} options.rpcApiHttpHost`);
        cactus_common_1.Checks.truthy(options.rpcApiWsHost, `${fnTag} options.rpcApiWsHost`);
        cactus_common_1.Checks.truthy(options.pluginRegistry, `${fnTag} options.pluginRegistry`);
        cactus_common_1.Checks.truthy(options.instanceId, `${fnTag} options.instanceId`);
        const { viemWebSocketTransportConfig } = options;
        this.logLevel = this.options.logLevel || "INFO";
        const label = this.className;
        this.log = cactus_common_1.LoggerProvider.getOrCreate({ level: this.logLevel, label });
        this.log.debug("Creating WebsocketProvider for %s", options.rpcApiWsHost);
        this.web3Provider = new web3_1.default.providers.WebsocketProvider(this.options.rpcApiWsHost);
        if (typeof viemWebSocketTransportConfig === "object") {
            this.viemWebSocketTransportConfig = {
                ...viemWebSocketTransportConfig,
                retryCount: Infinity,
                retryDelay: 1000,
                timeout: 2147483647,
                keepAlive: true,
            };
        }
        else {
            this.viemWebSocketTransportConfig = {
                retryCount: Infinity,
                retryDelay: 1000,
                timeout: 2147483647,
                keepAlive: true,
            };
        }
        const besuChain = (0, viem_1.defineChain)({
            id: options.networkId || 1337,
            name: "Besu",
            network: "besu-network",
            nativeCurrency: {
                decimals: 18,
                name: "Ether",
                symbol: "ETH",
            },
            rpcUrls: {
                default: {
                    http: [this.options.rpcApiHttpHost],
                    websocket: [this.options.rpcApiWsHost],
                },
            },
        });
        this.viemTransport = (0, viem_1.webSocket)(this.options.rpcApiWsHost, this.viemWebSocketTransportConfig);
        this.viemClient = (0, viem_1.createPublicClient)({
            chain: besuChain,
            transport: this.viemTransport,
        });
        this.web3 = new web3_1.default(this.web3Provider);
        this.instanceId = options.instanceId;
        this.pluginRegistry = options.pluginRegistry;
        this.prometheusExporter =
            options.prometheusExporter ||
                new prometheus_exporter_1.PrometheusExporter({ pollingIntervalInMin: 1 });
        cactus_common_1.Checks.truthy(this.prometheusExporter, `${fnTag} options.prometheusExporter`);
        this.prometheusExporter.startMetricsCollection();
    }
    getOpenApiSpec() {
        return openapi_json_1.default;
    }
    getPrometheusExporter() {
        return this.prometheusExporter;
    }
    async getPrometheusExporterMetrics() {
        const res = await this.prometheusExporter.getPrometheusMetrics();
        this.log.debug(`getPrometheusExporterMetrics() response: %o`, res);
        return res;
    }
    getInstanceId() {
        return this.instanceId;
    }
    getTxSubjectObservable() {
        return this.txSubject.asObservable();
    }
    async onPluginInit() {
        this.web3Quorum = (0, web3js_quorum_1.default)(this.web3);
        this.log.info("onPluginInit() querying networkId...");
        const networkId = await this.web3.eth.net.getId();
        this.log.info("onPluginInit() obtained networkId: %d", networkId);
        // Set up WebSocket connection monitoring and automated reconnection
        this.setupWebSocketConnection();
    }
    setupWebSocketConnection() {
        // Add event handlers for WebSocket connection
        this.web3Provider.on('error', (err) => {
            this.log.error('WebSocket error:', err);
        });
        this.web3Provider.on('end', () => {
            this.log.warn('WebSocket connection ended');
            // Try to reconnect immediately when the connection ends
            this.attemptReconnection();
        });
        this.web3Provider.on('connect', () => {
            this.log.info('WebSocket connected');
        });
        this.web3Provider.on('reconnect', (attempt) => {
            this.log.info('WebSocket reconnecting... Attempt:', attempt);
        });
        // Set up a more frequent heartbeat (every 30 seconds)
        // This helps keep the connection alive by showing activity
        const heartbeatInterval = 30000; // 30 seconds
        setInterval(() => this.sendHeartbeat(), heartbeatInterval);
    }
    attemptReconnection() {
        this.log.info("Attempting to reconnect WebSocket...");
        try {
            // Check if connection exists and its state
            if (this.web3Provider.connection) {
                const connection = this.web3Provider.connection;
                // WebSocket states: 0 = CONNECTING, 1 = OPEN, 2 = CLOSING, 3 = CLOSED
                if (connection.readyState === 3 || connection.readyState === 2) {
                    this.log.info("WebSocket is closed or closing, reconnecting...");
                    this.web3Provider.reconnect();
                }
            }
            else {
                // If connection doesn't exist, try to reconnect
                this.log.info("WebSocket connection object doesn't exist, reconnecting...");
                this.web3Provider.reconnect();
            }
        }
        catch (error) {
            const errorMessage = error instanceof Error ? error.message : String(error);
            this.log.error("Failed to reconnect WebSocket:", errorMessage);
        }
    }
    async sendHeartbeat() {
        try {
            // Check if the connection is open before sending the heartbeat
            const connection = this.web3Provider.connection;
            if (connection && connection.readyState === 1) { // 1 = OPEN
                // Use a lightweight call to keep the connection active
                await this.web3.eth.getBlockNumber()
                    .then(() => this.log.debug("WebSocket heartbeat sent"))
                    .catch((err) => {
                    this.log.error("WebSocket heartbeat failed:", err);
                    this.attemptReconnection();
                });
            }
            else {
                this.log.warn("WebSocket connection not open, attempting to reconnect...");
                this.attemptReconnection();
            }
        }
        catch (error) {
            const errorMessage = error instanceof Error ? error.message : String(error);
            this.log.error("Error in WebSocket heartbeat:", errorMessage);
            this.attemptReconnection();
        }
    }
    async shutdown() {
        this.log.info(`Shutting down...`);
        const rpcClient = await this.viemClient.transport.getRpcClient();
        this.log.debug("RPC client obtained.");
        rpcClient.close();
        this.log.debug("RPC client closed.");
        this.log.info(`shutdown complete.`);
    }
    async registerWebServices(app, wsApi) {
        const { web3, viemClient } = this;
        const { logLevel } = this.options;
        const webServices = await this.getOrCreateWebServices();
        await Promise.all(webServices.map((ws) => ws.registerExpress(app)));
        wsApi.on("connection", (socket) => {
            this.log.debug(`New Socket connected. ID=${socket.id}`);
            socket.on(typescript_axios_1.WatchBlocksV1.Subscribe, () => {
                new watch_blocks_v1_endpoint_1.WatchBlocksV1Endpoint({ web3, socket, logLevel }).subscribe();
            });
            socket.on(index_1.WatchEventsV1.Subscribe, (req) => {
                new watch_events_v1_endpoint_1.WatchEventsV1Endpoint({ viemClient, socket, logLevel }).subscribe(req);
            });
        });
        return webServices;
    }
    async createGrpcSvcDefAndImplPairs() {
        const openApiSvc = await this.createGrpcOpenApiSvcDefAndImplPair();
        const streamsSvc = await this.createGrpcStreamsSvcDefAndImplPair();
        return [openApiSvc, streamsSvc];
    }
    async createGrpcStreamsSvcDefAndImplPair() {
        const definition = besu_grpc_svc_streams.org.hyperledger.cacti.plugin.ledger.connector.besu
            .services.besuservice.UnimplementedBesuGrpcSvcStreamsService.definition;
        const implementation = new besu_grpc_svc_streams_1.BesuGrpcSvcStreams({
            logLevel: this.logLevel,
            web3: this.web3,
        });
        return { definition, implementation };
    }
    /**
     * Create a new instance of the service implementation.
     * Note: This does not cache the returned objects internally. A new instance
     * is created during every invocation.
     *
     * @returns The gRPC service definition+implementation pair that is backed
     * by the code generated by the OpenAPI generator from the openapi.json spec
     * of this package. Used by the API server to obtain the service objects dynamically
     * at runtime so that the plugin's gRPC services can be exposed in a similar
     * fashion how the HTTP REST endpoints are registered as well.
     */
    async createGrpcOpenApiSvcDefAndImplPair() {
        const definition = grpc_default_service.org.hyperledger.cacti.plugin.ledger.connector.besu
            .services.defaultservice.DefaultServiceClient.service;
        const implementation = new besu_grpc_svc_open_api_1.BesuGrpcSvcOpenApi({
            logLevel: this.logLevel,
            web3: this.web3,
        });
        return { definition, implementation };
    }
    async getOrCreateWebServices() {
        if (Array.isArray(this.endpoints)) {
            return this.endpoints;
        }
        const endpoints = [];
        {
            const endpoint = new deploy_contract_solidity_bytecode_endpoint_1.DeployContractSolidityBytecodeEndpoint({
                connector: this,
                logLevel: this.options.logLevel,
            });
            endpoints.push(endpoint);
        }
        {
            const endpoint = new deploy_contract_solidity_bytecode_no_keychain_endpoint_1.DeployContractSolidityBytecodeNoKeychainEndpoint({
                connector: this,
                logLevel: this.options.logLevel,
            });
            endpoints.push(endpoint);
        }
        {
            const endpoint = new get_balance_endpoint_1.GetBalanceEndpoint({
                connector: this,
                logLevel: this.options.logLevel,
            });
            endpoints.push(endpoint);
        }
        {
            const endpoint = new get_transaction_endpoint_1.GetTransactionEndpoint({
                connector: this,
                logLevel: this.options.logLevel,
            });
            endpoints.push(endpoint);
        }
        {
            const endpoint = new get_past_logs_endpoint_1.GetPastLogsEndpoint({
                connector: this,
                logLevel: this.options.logLevel,
            });
            endpoints.push(endpoint);
        }
        {
            const endpoint = new run_transaction_endpoint_1.RunTransactionEndpoint({
                connector: this,
                logLevel: this.options.logLevel,
            });
            endpoints.push(endpoint);
        }
        {
            const endpoint = new get_block_v1_endpoint_1.GetBlockEndpoint({
                connector: this,
                logLevel: this.options.logLevel,
            });
            endpoints.push(endpoint);
        }
        {
            const endpoint = new invoke_contract_endpoint_1.InvokeContractEndpoint({
                connector: this,
                logLevel: this.options.logLevel,
            });
            endpoints.push(endpoint);
        }
        {
            const endpoint = new sign_transaction_endpoint_v1_1.BesuSignTransactionEndpointV1({
                connector: this,
                logLevel: this.options.logLevel,
            });
            endpoints.push(endpoint);
        }
        {
            const endpoint = new get_besu_record_endpoint_v1_1.GetBesuRecordEndpointV1({
                connector: this,
                logLevel: this.options.logLevel,
            });
            endpoints.push(endpoint);
        }
        {
            const opts = {
                connector: this,
                logLevel: this.options.logLevel,
            };
            const endpoint = new get_prometheus_exporter_metrics_endpoint_v1_1.GetPrometheusExporterMetricsEndpointV1(opts);
            endpoints.push(endpoint);
        }
        {
            const oasPath = openapi_json_1.default.paths["/api/v1/plugins/@hyperledger/cactus-plugin-ledger-connector-besu/get-open-api-spec"];
            const operationId = oasPath.get.operationId;
            const opts = {
                oas: openapi_json_1.default,
                oasPath,
                operationId,
                path: oasPath.get["x-hyperledger-cacti"].http.path,
                pluginRegistry: this.pluginRegistry,
                verbLowerCase: oasPath.get["x-hyperledger-cacti"].http.verbLowerCase,
                logLevel: this.options.logLevel,
            };
            const endpoint = new get_open_api_spec_v1_endpoint_1.GetOpenApiSpecV1Endpoint(opts);
            endpoints.push(endpoint);
        }
        this.endpoints = endpoints;
        return endpoints;
    }
    getPackageName() {
        return `@hyperledger/cactus-plugin-ledger-connector-besu`;
    }
    async getConsensusAlgorithmFamily() {
        return cactus_core_api_1.ConsensusAlgorithmFamily.Authority;
    }
    async hasTransactionFinality() {
        const currentConsensusAlgorithmFamily = await this.getConsensusAlgorithmFamily();
        return (0, cactus_core_1.consensusHasTransactionFinality)(currentConsensusAlgorithmFamily);
    }
    /**
     * Verifies that it is safe to call a specific method of a Web3 Contract.
     *
     * @param contract The Web3 Contract instance to check whether it has a method with a specific name or not.
     * @param name The name of the method that will be checked if it's usable on `contract` or not.
     * @returns Boolean `true` when it IS safe to call the method named `name` on the contract.
     * @throws If the contract instance is falsy or it's methods object is falsy. Also throws if the method name is a blank string.
     */
    async isSafeToCallContractMethod(contract, name) {
        cactus_common_1.Checks.truthy(contract, `${this.className}#isSafeToCallContractMethod():contract`);
        cactus_common_1.Checks.truthy(contract.methods, `${this.className}#isSafeToCallContractMethod():contract.methods`);
        cactus_common_1.Checks.nonBlankString(name, `${this.className}#isSafeToCallContractMethod():name`);
        const { methods } = contract;
        return Object.prototype.hasOwnProperty.call(methods, name);
    }
    async invokeContract(req) {
        const fnTag = `${this.className}#invokeContract()`;
        const contractName = req.contractName;
        let contractInstance;
        if (req.keychainId != undefined) {
            const networkId = await this.web3.eth.net.getId();
            const keychainPlugin = this.pluginRegistry.findOneByKeychainId(req.keychainId);
            cactus_common_1.Checks.truthy(keychainPlugin, `${fnTag} keychain for ID:"${req.keychainId}"`);
            if (!keychainPlugin.has(contractName)) {
                throw new Error(`${fnTag} Cannot create an instance of the contract because the contractName and the contractName of the JSON doesn't match`);
            }
            const contractStr = await keychainPlugin.get(contractName);
            const contractJSON = JSON.parse(contractStr);
            if (contractJSON.networks === undefined ||
                contractJSON.networks[networkId] === undefined ||
                contractJSON.networks[networkId].address === undefined) {
                if ((0, model_type_guards_1.isWeb3SigningCredentialNone)(req.signingCredential)) {
                    throw new Error(`${fnTag} Cannot deploy contract with pre-signed TX`);
                }
                const web3SigningCredential = req.signingCredential;
                const receipt = await this.transact({
                    transactionConfig: {
                        data: `0x${contractJSON.bytecode}`,
                        from: web3SigningCredential.ethAccount,
                        gas: req.gas,
                        gasPrice: req.gasPrice,
                    },
                    consistencyStrategy: {
                        blockConfirmations: 0,
                        receiptType: typescript_axios_1.ReceiptType.NodeTxPoolAck,
                        timeoutMs: req.timeoutMs || 60000,
                    },
                    web3SigningCredential,
                    privateTransactionConfig: req.privateTransactionConfig,
                });
                const address = {
                    address: receipt.transactionReceipt.contractAddress,
                };
                const network = { [networkId]: address };
                contractJSON.networks = network;
                keychainPlugin.set(contractName, JSON.stringify(contractJSON));
            }
            const contract = new this.web3.eth.Contract(contractJSON.abi, contractJSON.networks[networkId].address);
            this.contracts[contractName] = contract;
        }
        else if (req.keychainId == undefined &&
            req.contractAbi == undefined &&
            req.contractAddress == undefined) {
            throw new Error(`${fnTag} Cannot invoke a contract without contract instance, the keychainId param is needed`);
        }
        contractInstance = this.contracts[contractName];
        if (req.contractAbi != undefined) {
            let abi;
            if (typeof req.contractAbi === "string") {
                abi = JSON.parse(req.contractAbi);
            }
            else {
                abi = req.contractAbi;
            }
            const { contractAddress } = req;
            contractInstance = new this.web3.eth.Contract(abi, contractAddress);
        }
        const isSafeToCall = await this.isSafeToCallContractMethod(contractInstance, req.methodName);
        if (!isSafeToCall) {
            throw new run_time_error_cjs_1.RuntimeError(`Invalid method name provided in request. ${req.methodName} does not exist on the Web3 contract object's "methods" property.`);
        }
        const methodRef = contractInstance.methods[req.methodName];
        cactus_common_1.Checks.truthy(methodRef, `${fnTag} YourContract.${req.methodName}`);
        const method = methodRef(...req.params);
        if (req.invocationType === typescript_axios_1.EthContractInvocationType.Call) {
            let callOutput;
            let success = false;
            if (req.privateTransactionConfig) {
                const data = method.encodeABI();
                let privKey;
                if (req.signingCredential.type ==
                    typescript_axios_1.Web3SigningCredentialType.CactusKeychainRef) {
                    const { keychainEntryKey, keychainId } = req.signingCredential;
                    const keychainPlugin = this.pluginRegistry.findOneByKeychainId(keychainId);
                    privKey = await keychainPlugin?.get(keychainEntryKey);
                }
                else {
                    privKey = req.signingCredential.secret;
                }
                const fnParams = {
                    to: contractInstance.options.address,
                    data,
                    privateFrom: req.privateTransactionConfig.privateFrom,
                    privateKey: privKey,
                    privateFor: req.privateTransactionConfig.privateFor,
                };
                if (!this.web3Quorum) {
                    throw new run_time_error_cjs_1.RuntimeError(`InvalidState: web3Quorum not initialized.`);
                }
                const privacyGroupId = this.web3Quorum.utils.generatePrivacyGroup(fnParams);
                this.log.debug("Generated privacyGroupId: ", privacyGroupId);
                callOutput = await this.web3Quorum.priv.call(privacyGroupId, {
                    to: contractInstance.options.address,
                    data,
                    // TODO: Update the "from" property of ICallOptions to be optional
                });
                success = true;
                this.log.debug(`Web3 EEA Call output: `, callOutput);
            }
            else {
                callOutput = await method.call();
                success = true;
            }
            return { success, callOutput };
        }
        else if (req.invocationType === typescript_axios_1.EthContractInvocationType.Send) {
            if ((0, model_type_guards_1.isWeb3SigningCredentialNone)(req.signingCredential)) {
                throw new Error(`${fnTag} Cannot deploy contract with pre-signed TX`);
            }
            const web3SigningCredential = req.signingCredential;
            const payload = method.send.request();
            const { params } = payload;
            const [transactionConfig] = params;
            transactionConfig.from = web3SigningCredential.ethAccount;
            if (req.gas == undefined) {
                req.gas = await this.web3.eth.estimateGas(transactionConfig);
            }
            transactionConfig.gas = req.gas;
            transactionConfig.gasPrice = req.gasPrice;
            transactionConfig.value = req.value;
            transactionConfig.nonce = req.nonce;
            const txReq = {
                transactionConfig,
                web3SigningCredential,
                consistencyStrategy: {
                    blockConfirmations: 0,
                    receiptType: typescript_axios_1.ReceiptType.NodeTxPoolAck,
                    timeoutMs: req.timeoutMs || 60000,
                },
                privateTransactionConfig: req.privateTransactionConfig,
            };
            const out = await this.transact(txReq);
            const success = out.transactionReceipt.status;
            const data = { success, out };
            // create IRunTransactionV1Exchange for transaction monitoring
            const receiptData = {
                request: req,
                response: out,
                timestamp: new Date(),
            };
            this.log.debug(`IRunTransactionV1Exchange created ${receiptData}`);
            this.txSubject.next(receiptData);
            return data;
        }
        else {
            throw new Error(`${fnTag} Unsupported invocation type ${req.invocationType}`);
        }
    }
    async transact(req) {
        const ctx = {
            prometheusExporter: this.prometheusExporter,
            pluginRegistry: this.pluginRegistry,
            logLevel: this.logLevel,
            web3: this.web3,
        };
        const runTransactionResponse = (0, transact_v1_impl_1.transactV1Impl)(ctx, req);
        return runTransactionResponse;
    }
    async deployContract(req) {
        const ctx = {
            pluginRegistry: this.pluginRegistry,
            prometheusExporter: this.prometheusExporter,
            web3: this.web3,
            logLevel: this.logLevel,
        };
        const res = await (0, deploy_contract_v1_keychain_1.deployContractV1Keychain)(ctx, req);
        const { status, contractAddress, contractName, contract } = res;
        if (status && contractAddress && contract) {
            this.contracts[contractName] = contract;
        }
        return res.deployResponse;
    }
    async deployContractNoKeychain(req) {
        const ctx = {
            pluginRegistry: this.pluginRegistry,
            prometheusExporter: this.prometheusExporter,
            web3: this.web3,
            logLevel: this.logLevel,
        };
        this.log.debug("Invoking deployContractV1NoKeychain()...");
        const res = (0, deploy_contract_v1_no_keychain_1.deployContractV1NoKeychain)(ctx, req);
        this.log.debug("Ran deployContractV1NoKeychain() OK");
        return res;
    }
    async signTransaction(req) {
        const { pluginRegistry, rpcApiHttpHost, logLevel } = this.options;
        const { keychainId, keychainRef, transactionHash } = req;
        const converter = new cactus_common_1.KeyConverter();
        const web3Provider = new web3_1.default.providers.HttpProvider(rpcApiHttpHost);
        const web3 = new web3_1.default(web3Provider);
        // Make sure the transaction exists on the ledger first...
        const transaction = await web3.eth.getTransaction(transactionHash);
        if (!transaction) {
            return typescript_optional_1.Optional.empty();
        }
        const keychain = pluginRegistry.findOneByKeychainId(keychainId);
        if (!keychain) {
            const msg = `Keychain for ID ${keychainId} not found.`;
            throw new cactus_common_1.CodedError(msg, exports.E_KEYCHAIN_NOT_FOUND);
        }
        const pem = await keychain.get(keychainRef);
        const pkRaw = converter.privateKeyAs(pem, cactus_common_1.KeyFormat.PEM, cactus_common_1.KeyFormat.Raw);
        const jsObjectSignerOptions = {
            privateKey: pkRaw,
            logLevel,
        };
        const jsObjectSigner = new cactus_common_1.JsObjectSigner(jsObjectSignerOptions);
        if (transaction !== undefined && transaction !== null) {
            const singData = jsObjectSigner.sign(transaction.input);
            const signDataHex = Buffer.from(singData).toString("hex");
            const resBody = { signature: signDataHex };
            return typescript_optional_1.Optional.ofNullable(resBody);
        }
        return typescript_optional_1.Optional.empty();
    }
    async getBalance(request) {
        const balance = await this.web3.eth.getBalance(request.address, request.defaultBlock);
        return { balance };
    }
    async getTransaction(request) {
        const transaction = await this.web3.eth.getTransaction(request.transactionHash);
        return { transaction };
    }
    async getPastLogs(request) {
        const logs = await this.web3.eth.getPastLogs(request);
        return { logs };
    }
    async getBlock(request) {
        const ctx = { logLevel: this.logLevel, web3: this.web3 };
        const getBlockV1Response = await (0, get_block_v1_http_1.getBlockV1Http)(ctx, request);
        this.log.debug("getBlockV1Response=%o", getBlockV1Response);
        return getBlockV1Response;
    }
    async getBesuRecord(request) {
        const fnTag = `${this.className}#getBesuRecord()`;
        //////////////////////////////////////////////
        let abi = [];
        const resp = {};
        const txHash = request.transactionHash;
        if (txHash) {
            const transaction = await this.web3.eth.getTransaction(txHash);
            if (transaction.input) {
                resp.transactionInputData = transaction.input;
                return resp;
            }
        }
        if (request.invokeCall) {
            if (request.invokeCall.contractAbi) {
                if (typeof request.invokeCall.contractAbi === "string") {
                    abi = JSON.parse(request.invokeCall.contractAbi);
                }
                else {
                    abi = request.invokeCall.contractAbi;
                }
            }
            const { contractAddress } = request.invokeCall;
            const contractInstance = new this.web3.eth.Contract(abi, contractAddress);
            const isSafeToCall = await this.isSafeToCallContractMethod(contractInstance, request.invokeCall.methodName);
            if (!isSafeToCall) {
                throw new run_time_error_cjs_1.RuntimeError(`Invalid method name provided in request. ${request.invokeCall.methodName} does not exist on the Web3 contract object's "methods" property.`);
            }
            const methodRef = contractInstance.methods[request.invokeCall.methodName];
            cactus_common_1.Checks.truthy(methodRef, `${fnTag} YourContract.${request.invokeCall.methodName}`);
            const method = methodRef(...request.invokeCall.params);
            if (request.invokeCall.invocationType === typescript_axios_1.EthContractInvocationType.Call) {
                const callOutput = await method.call();
                const res = {
                    callOutput,
                };
                return res;
            }
            else {
                throw new Error(`${fnTag} Unsupported invocation type ${request.invokeCall.invocationType}`);
            }
        }
        return resp;
    }
}
exports.PluginLedgerConnectorBesu = PluginLedgerConnectorBesu;
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoicGx1Z2luLWxlZGdlci1jb25uZWN0b3ItYmVzdS5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uL3NyYy9tYWluL3R5cGVzY3JpcHQvcGx1Z2luLWxlZGdlci1jb25uZWN0b3ItYmVzdS50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7OztBQUNBLDJEQUFrRDtBQUNsRCwrQkFBaUQ7QUFHakQsNkRBQStDO0FBQy9DLCtCQUtjO0FBR2QsZ0RBQXdCO0FBR3hCLGtFQUEwRDtBQUcxRCxrRUFTc0M7QUFFdEMsMERBR2tDO0FBRWxDLDhEQVVvQztBQUVwQywwSEFBbUg7QUFDbkgsa0pBQXlJO0FBRXpJLHNFQU1vRDtBQU9wRCwyRUFxQjhDO0FBRTlDLHNGQUFpRjtBQUNqRiwyREFBa0U7QUFDbEUsOEZBQTRGO0FBQzVGLG1GQUErRTtBQUMvRSw0SEFHb0U7QUFDcEUsc0ZBQWdGO0FBQ2hGLDhFQUF5RTtBQUN6RSxzRkFBaUY7QUFDakYsa0ZBQTRFO0FBQzVFLHNGQUFpRjtBQUNqRixpRkFBeUU7QUFDekUsNEZBQXFGO0FBQ3JGLGdHQUdzRDtBQUN0RCwrR0FBaUc7QUFDakcsc0hBQXdHO0FBQ3hHLG1GQUE0RTtBQUM1RSxpRkFBMkU7QUFDM0UsNkVBQXVFO0FBQ3ZFLDBFQUFxRTtBQUNyRSx1R0FBaUc7QUFDakcsNkdBQXNHO0FBQ3RHLHNGQUFnRjtBQUNoRix3RUFBdUM7QUFRMUIsUUFBQSxvQkFBb0IsR0FBRywwQ0FBMEMsQ0FBQztBQWMvRSxNQUFhLHlCQUF5QjtJQXFDUjtJQXpCWCxVQUFVLENBQVM7SUFDN0Isa0JBQWtCLENBQXFCO0lBQzdCLEdBQUcsQ0FBUztJQUNaLFFBQVEsQ0FBZTtJQUN2QixZQUFZLENBQW9CO0lBQ2hDLElBQUksQ0FBTztJQUNYLFVBQVUsQ0FBbUI7SUFDN0IsYUFBYSxDQUF5QjtJQUN0Qyw0QkFBNEIsQ0FBb0M7SUFDekUsVUFBVSxDQUEwQjtJQUMzQixj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