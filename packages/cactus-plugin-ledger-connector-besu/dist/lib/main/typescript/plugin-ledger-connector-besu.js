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
    reconnectionAttempts = 0;
    reconnectionInProgress = false;
    reconnectionTimeoutId;
    heartbeatIntervalId;
    maxReconnectionAttempts = 15;
    initialBackoffMs = 2000;
    maxBackoffMs = 120000;
    setupWebSocketConnection() {
        this.web3Provider.on('error', (err) => {
            this.log.error('WebSocket error:', err);
        });
        this.web3Provider.on('end', () => {
            this.log.warn('WebSocket connection ended');
            this.attemptReconnection();
        });
        this.web3Provider.on('connect', () => {
            this.log.info('WebSocket connected successfully');
            // Clear any pending reconnection attempts
            if (this.reconnectionTimeoutId) {
                clearTimeout(this.reconnectionTimeoutId);
                this.reconnectionTimeoutId = undefined;
            }
            this.reconnectionAttempts = 0;
            this.reconnectionInProgress = false;
        });
        this.web3Provider.on('reconnect', (attempt) => {
            this.log.info('WebSocket reconnecting... Attempt:', attempt);
        });
        // Heartbeat every 30 seconds
        this.heartbeatIntervalId = setInterval(() => this.sendHeartbeat(), 30000);
    }
    /**
     * Attempts to reconnect the WebSocket connection using an exponential backoff strategy.
     * Delay starts at 2 seconds and doubles with each attempt (2s, 4s, 8s, 16s...) up to 2 minutes maximum.
     */
    attemptReconnection() {
        // Guard: check if already reconnecting
        if (this.reconnectionInProgress) {
            this.log.debug("Reconnection already in progress, skipping duplicate attempt");
            return;
        }
        this.reconnectionInProgress = true;
        const currentAttempt = this.reconnectionAttempts + 1;
        this.log.info(`Attempting to reconnect WebSocket... (Attempt ${currentAttempt}/${this.maxReconnectionAttempts})`);
        // Check if max attempts reached
        if (this.reconnectionAttempts >= this.maxReconnectionAttempts) {
            this.log.error(`Failed to reconnect after ${this.maxReconnectionAttempts} attempts. Recreating provider...`);
            this.recreateWebSocketProvider();
            this.reconnectionAttempts = 0;
            this.reconnectionInProgress = false;
            return;
        }
        try {
            if (!this.web3Provider.connection) {
                this.log.info("Connection object doesn't exist, recreating provider...");
                this.recreateWebSocketProvider();
                this.reconnectionAttempts = 0;
                this.reconnectionInProgress = false;
                return;
            }
            const connection = this.web3Provider.connection;
            // WebSocket states: 0 = CONNECTING, 1 = OPEN, 2 = CLOSING, 3 = CLOSED
            if (connection.readyState === 1) {
                this.log.info("Connection is already open");
                this.reconnectionAttempts = 0;
                this.reconnectionInProgress = false;
                return;
            }
            if (connection.readyState === 0) {
                // CONNECTING - wait and check again
                this.log.info("Connection is in CONNECTING state, waiting...");
                this.reconnectionTimeoutId = setTimeout(() => {
                    this.reconnectionInProgress = false;
                    this.attemptReconnection();
                }, 2000);
                return;
            }
            // readyState === 2 (CLOSING) lub 3 (CLOSED)
            this.log.info("WebSocket is closed or closing, scheduling reconnect...");
            const backoffTime = Math.min(this.initialBackoffMs * Math.pow(2, this.reconnectionAttempts), this.maxBackoffMs);
            this.log.info(`Using backoff delay of ${backoffTime / 1000} seconds`);
            this.reconnectionAttempts++;
            this.reconnectionTimeoutId = setTimeout(() => {
                this.scheduleReconnectAttempt();
            }, backoffTime);
        }
        catch (error) {
            const errorMessage = error instanceof Error ? error.message : String(error);
            this.log.error(`Error in attemptReconnection: ${errorMessage}`);
            this.reconnectionInProgress = false;
            this.reconnectionAttempts++;
            const backoffTime = Math.min(this.initialBackoffMs * Math.pow(2, this.reconnectionAttempts), this.maxBackoffMs);
            this.log.info(`Will retry in ${backoffTime / 1000} seconds...`);
            this.reconnectionTimeoutId = setTimeout(() => {
                this.attemptReconnection();
            }, backoffTime);
        }
    }
    /**
     * Executes the actual reconnect call on the WebSocket provider.
     */
    scheduleReconnectAttempt() {
        try {
            this.log.info("Executing reconnect attempt now...");
            // Check if provider still exists
            if (!this.web3Provider.connection) {
                this.log.warn("Connection lost during backoff, recreating provider...");
                this.reconnectionInProgress = false;
                this.recreateWebSocketProvider();
                return;
            }
            this.web3Provider.reconnect();
            // Wait 5 seconds for reconnect, then check status
            this.reconnectionTimeoutId = setTimeout(() => {
                this.checkReconnectionStatus();
            }, 5000);
        }
        catch (error) {
            const errorMessage = error instanceof Error ? error.message : String(error);
            this.log.error(`Failed to execute reconnect: ${errorMessage}`);
            this.reconnectionInProgress = false;
            // If reconnect threw an exception, try again
            if (this.reconnectionAttempts < this.maxReconnectionAttempts) {
                this.attemptReconnection();
            }
            else {
                this.log.error("Max attempts reached, recreating provider...");
                this.recreateWebSocketProvider();
            }
        }
    }
    /**
     * Checks if reconnection was successful
     */
    checkReconnectionStatus() {
        try {
            const conn = this.web3Provider.connection;
            if (!conn) {
                this.log.warn("Connection object disappeared, recreating provider...");
                this.reconnectionInProgress = false;
                this.recreateWebSocketProvider();
                return;
            }
            if (conn.readyState === 1) {
                this.log.info("Reconnection successful!");
                this.reconnectionAttempts = 0;
                this.reconnectionInProgress = false;
                return;
            }
            // Failed - try again
            this.log.warn(`Reconnection failed, connection state: ${conn.readyState}`);
            this.reconnectionInProgress = false;
            if (this.reconnectionAttempts < this.maxReconnectionAttempts) {
                this.attemptReconnection();
            }
            else {
                this.log.error("Max attempts reached, recreating provider...");
                this.reconnectionAttempts = 0;
                this.recreateWebSocketProvider();
            }
        }
        catch (error) {
            const errorMessage = error instanceof Error ? error.message : String(error);
            this.log.error(`Error checking reconnection status: ${errorMessage}`);
            this.reconnectionInProgress = false;
            this.attemptReconnection();
        }
    }
    /**
     * Completely recreates the WebSocket provider from scratch.
     * This is used as a fallback when reconnection attempts fail.
     */
    recreateWebSocketProvider() {
        this.log.info("Recreating WebSocket provider from scratch...");
        if (this.reconnectionTimeoutId) {
            clearTimeout(this.reconnectionTimeoutId);
            this.reconnectionTimeoutId = undefined;
        }
        try {
            // Disconnect old
            try {
                if (this.web3Provider.connection) {
                    this.web3Provider.disconnect();
                }
            }
            catch (err) {
                this.log.debug("Error disconnecting old provider:", err);
            }
            // Create new
            const newProvider = new web3_1.default.providers.WebsocketProvider(this.options.rpcApiWsHost);
            // Setup handlers (bez zmian)
            newProvider.on('error', (err) => {
                this.log.error('WebSocket error:', err);
            });
            newProvider.on('end', () => {
                this.log.warn('WebSocket connection ended');
                this.attemptReconnection();
            });
            newProvider.on('connect', () => {
                this.log.info('WebSocket connected successfully');
                if (this.reconnectionTimeoutId) {
                    clearTimeout(this.reconnectionTimeoutId);
                    this.reconnectionTimeoutId = undefined;
                }
                this.reconnectionAttempts = 0;
                this.reconnectionInProgress = false;
            });
            newProvider.on('reconnect', (attempt) => {
                this.log.info('WebSocket reconnecting... Attempt:', attempt);
            });
            this.web3.setProvider(newProvider);
            this.web3Provider = newProvider;
            this.log.info("New WebSocket provider created and attached");
            this.reconnectionAttempts = 0;
            this.reconnectionInProgress = false;
        }
        catch (error) {
            const errorMessage = error instanceof Error ? error.message : String(error);
            this.log.error(`Failed to recreate WebSocket provider: ${errorMessage}`);
            // Force another attempt after 5 seconds
            this.reconnectionTimeoutId = setTimeout(() => {
                this.reconnectionInProgress = false;
                this.reconnectionAttempts = 0;
                this.attemptReconnection();
            }, 5000);
        }
    }
    async sendHeartbeat() {
        try {
            // Skip if already reconnecting
            if (this.reconnectionInProgress) {
                this.log.debug("Reconnection in progress, skipping heartbeat");
                return;
            }
            const connection = this.web3Provider.connection;
            // Skip if provider is reconnecting
            if (this.web3Provider.reconnecting) {
                this.log.debug("Provider reconnecting, skipping heartbeat");
                return;
            }
            if (!connection) {
                this.log.warn("No connection object, attempting to reconnect...");
                this.attemptReconnection();
                return;
            }
            if (connection.readyState === 1) {
                await this.web3.eth.getBlockNumber()
                    .then(() => this.log.debug("WebSocket heartbeat sent"))
                    .catch((err) => {
                    // Ignore reconnection errors
                    if (err.message?.includes('reconnect') || err.message?.includes('CONNECTION ERROR')) {
                        this.log.debug("Heartbeat failed due to reconnection - expected");
                        return;
                    }
                    this.log.error("WebSocket heartbeat failed:", err);
                    this.attemptReconnection();
                });
            }
            else {
                this.log.warn(`WebSocket connection not open (state: ${connection.readyState})`);
                this.attemptReconnection();
            }
        }
        catch (error) {
            const errorMessage = error instanceof Error ? error.message : String(error);
            // Don't log as error if related to reconnect
            if (errorMessage.includes('reconnect') || errorMessage.includes('CONNECTION ERROR')) {
                this.log.debug("Heartbeat error during reconnection:", errorMessage);
                return;
            }
            this.log.error("Error in WebSocket heartbeat:", errorMessage);
            this.attemptReconnection();
        }
    }
    async shutdown() {
        this.log.info(`Shutting down...`);
        // Stop heartbeat
        if (this.heartbeatIntervalId) {
            clearInterval(this.heartbeatIntervalId);
            this.heartbeatIntervalId = undefined;
        }
        // Clear reconnection timeouts
        if (this.reconnectionTimeoutId) {
            clearTimeout(this.reconnectionTimeoutId);
            this.reconnectionTimeoutId = undefined;
        }
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
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoicGx1Z2luLWxlZGdlci1jb25uZWN0b3ItYmVzdS5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uL3NyYy9tYWluL3R5cGVzY3JpcHQvcGx1Z2luLWxlZGdlci1jb25uZWN0b3ItYmVzdS50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7OztBQUNBLDJEQUFrRDtBQUNsRCwrQkFBaUQ7QUFHakQsNkRBQStDO0FBQy9DLCtCQUtjO0FBR2QsZ0RBQXdCO0FBR3hCLGtFQUEwRDtBQUcxRCxrRUFTc0M7QUFFdEMsMERBR2tDO0FBRWxDLDhEQVVvQztBQUVwQywwSEFBbUg7QUFDbkgsa0pBQXlJO0FBRXpJLHNFQU1vRDtBQU9wRCwyRUFxQjhDO0FBRTlDLHNGQUFpRjtBQUNqRiwyREFBa0U7QUFDbEUsOEZBQTRGO0FBQzVGLG1GQUErRTtBQUMvRSw0SEFHb0U7QUFDcEUsc0ZBQWdGO0FBQ2hGLDhFQUF5RTtBQUN6RSxzRkFBaUY7QUFDakYsa0ZBQTRFO0FBQzVFLHNGQUFpRjtBQUNqRixpRkFBeUU7QUFDekUsNEZBQXFGO0FBQ3JGLGdHQUdzRDtBQUN0RCwrR0FBaUc7QUFDakcsc0hBQXdHO0FBQ3hHLG1GQUE0RTtBQUM1RSxpRkFBMkU7QUFDM0UsNkVBQXVFO0FBQ3ZFLDBFQUFxRTtBQUNyRSx1R0FBaUc7QUFDakcsNkdBQXNHO0FBQ3RHLHNGQUFnRjtBQUNoRix3RUFBdUM7QUFRMUIsUUFBQSxvQkFBb0IsR0FBRywwQ0FBMEMsQ0FBQztBQWMvRSxNQUFhLHlCQUF5QjtJQXFDUjtJQXpCWCxVQUFVLENBQVM7SUFDN0Isa0JBQWtCLENBQXFCO0lBQzdCLEdBQUcsQ0FBUztJQUNaLFFBQVEsQ0FBZTtJQUNoQyxZQUFZLENBQW9CO0lBQ3ZCLElBQUksQ0FBTztJQUNYLFVBQVUsQ0FBbUI7SUFDN0IsYUFBYSxDQUF5QjtJQUN0Qyw0QkFBNEIsQ0FBb0M7SUFDekUsVUFBVSxDQUEwQjtJQUMzQixj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QUN0QyxTQUFTLEVBQUUsSUFBSTtnQkFDZixRQUFRLEVBQUUsSUFBSSxDQUFDLE9BQU8sQ0FBQyxRQUFRO2FBQ2hDLENBQUMsQ0FBQztZQUNILFNBQVMsQ0FBQyxJQUFJLENBQUMsUUFBUSxDQUFDLENBQUM7UUFDM0IsQ0FBQztRQUNELENBQUM7WUFDQyxNQUFNLFFBQVEsR0FBRyxJQUFJLGlEQUFzQixDQUFDO2dCQUMxQyxTQUFTLEVBQUUsSUFBSTtnQkFDZixRQUFRLEVBQUUsSUFBSSxDQUFDLE9BQU8sQ0FBQyxRQUFRO2FBQ2hDLENBQUMsQ0FBQztZQUNILFNBQVMsQ0FBQyxJQUFJLENBQUMsUUFBUSxDQUFDLENBQUM7UUFDM0IsQ0FBQztRQUNELENBQUM7WUFDQyxNQUFNLFFBQVEsR0FBRyxJQUFJLDRDQUFtQixDQUFDO2dCQUN2QyxTQUFTLEVBQUUsSUFBSTtnQkFDZixRQUFRLEVBQUUsSUFBSSxDQUFDLE9BQU8sQ0FBQyxRQUFRO2FBQ2hDLENBQUMsQ0FBQztZQUNILFNBQVMsQ0FBQyxJQUFJLENBQUMsUUFBUSxDQUFDLENBQUM7UUFDM0IsQ0FBQztRQUNELENBQUM7WUFDQyxNQUFNLFFBQVEsR0FBRyxJQUFJLGlEQUFzQixDQUFDO2dCQUMxQyxTQUFTLEVBQUUsSUFBSTtnQkFDZixRQUFRLEVBQUUsSUFBSSxDQUFDLE9BQU8sQ0FBQyxRQUFRO2FBQ2hDLENBQUMsQ0FBQztZQUNILFNBQVMsQ0FBQyxJQUFJLENBQUMsUUFBUSxDQUFDLENBQUM7UUFDM0IsQ0FBQztRQUNELENBQUM7WUFDQyxNQUFNLFFBQVEsR0FBRyxJQUFJLHdDQUFnQixDQUFDO2dCQUNwQyxTQUFTLEVBQUUsSUFBSTtnQkFDZixRQUFRLEVBQUUsSUFBSSxDQUFDLE9BQU8sQ0FBQyxRQUFRO2FBQ2hDLENBQUMsQ0FBQztZQUNILFNBQVMsQ0FBQyxJQUFJLENBQUMsUUFBUSxDQUFDLENBQUM7UUFDM0IsQ0FBQztRQUNELENBQUM7WUFDQyxNQUFNLFFBQVEsR0FBRyxJQUFJLGlEQUFzQixDQUFDO2dCQUMxQyxTQUFTLEVBQUUs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