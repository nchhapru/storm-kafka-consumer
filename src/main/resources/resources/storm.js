/**
  * Base classes in node-js for storm Bolt and Spout.
  * Implements the storm multilang protocol for nodejs.
  */
 
 var fs = require('fs');
 
 function Storm() {
     this.lines = [];
     this.taskIdsCallbacks = [];
     this.isFirstMessage = true;
 }
 
 Storm.prototype.sendMsgToParent = function(msg) {
     var str = JSON.stringify(msg).concat('\nend\n');
     process.stdout.write(str);
 }
 
 Storm.prototype.sync = function() {
     this.sendMsgToParent({"command":"sync"});
 }
 
 Storm.prototype.sendPid = function(heartbeatdir) {
     var pid = process.pid;
     this.sendMsgToParent({"pid": pid})
     fs.closeSync(fs.openSync(heartbeatdir.concat("/").concat(pid), "w"));
 }
 
 Storm.prototype.log = function(msg) {
     this.sendMsgToParent({"command": "log", "msg": msg});
 }
 
 Storm.prototype.initSetupInfo = function(setupInfo) {
     var self = this;
     var callback = function() {
         self.sendPid(setupInfo['pidDir']);
     }
     this.initialize(setupInfo['conf'], setupInfo['context'], callback);
 }
 
 Storm.prototype.startReadingInput = function() {
     var self = this;
 
     process.stdin.on('readable', function() {
         var chunk = process.stdin.read();
 
         if (chunk && chunk.length !== 0) {
           var lines = chunk.toString().split('\n');
           lines.forEach(function(line) {
               self.handleNewLine(line);
           })
         }
     });
 }
 
 Storm.prototype.handleNewLine = function(line) {
     if (line === 'end') {
         var msg = this.collectMessageLines();
         this.cleanLines();
         this.handleNewMessage(msg);
     } else {
         this.storeLine(line);
     }
 }
 
 Storm.prototype.collectMessageLines = function() {
     return this.lines.join('\n');
 }
 
 Storm.prototype.cleanLines = function() {
     this.lines = [];
 }
 
 Storm.prototype.storeLine = function(line) {
     this.lines.push(line);
 }
 
 Storm.prototype.isTaskIds = function(msg) {
     return (msg instanceof Array);
 }
 
 Storm.prototype.handleNewMessage = function(msg) {
     var parsedMsg = JSON.parse(msg);
 
     if (this.isFirstMessage) {
         this.initSetupInfo(parsedMsg);
         this.isFirstMessage = false;
     } else if (this.isTaskIds(parsedMsg)) {
         this.handleNewTaskId(parsedMsg);
     } else {
         this.handleNewCommand(parsedMsg);
     }
 }
 
 Storm.prototype.handleNewTaskId = function(taskIds) {
     //When new list of task ids arrives, the callback that was passed with the corresponding emit should be called.
     //Storm assures that the task ids will be sent in the same order as their corresponding emits so it we can simply
     //take the first callback in the list and be sure it is the right one.
 
     var callback = this.taskIdsCallbacks.shift();
     if (callback) {
         callback(taskIds);
     } else {
         throw new Error('Something went wrong, we off the split of task id callbacks');
     }
 }
 
 
 /**
  *
  * @param messageDetails json with the emit details.
  *
  * For bolt, the json must contain the required fields:
  * - tuple - the value to emit
  * - anchorTupleId - the value of the anchor tuple (the input tuple that lead to this emit). Used to track the source
  * tuple and return ack when all components successfully finished to process it.
  * and may contain the optional fields:
  * - stream (if empty - emit to default stream)
  *
  * For spout, the json must contain the required fields:
  * - tuple - the value to emit
  *
  * and may contain the optional fields:
  * - id - pass id for reliable emit (and receive ack/fail later).
  * - stream - if empty - emit to default stream.
  *
  * @param onTaskIds function than will be called with list of task ids the message was emitted to (when received).
  */
 Storm.prototype.emit = function(messageDetails, onTaskIds) {
     //Every emit triggers a response - list of task ids to which the tuple was emitted. The task ids are accessible
     //through the callback (will be called when the response arrives). The callback is stored in a list until the
     //corresponding task id list arrives.
     if (messageDetails.task) {
         throw new Error('Illegal input - task. To emit to specific task use emit direct!');
     }
 
     if (!onTaskIds) {
         throw new Error('You must pass a onTaskIds callback when using emit!')
     }
 
     this.taskIdsCallbacks.push(onTaskIds);
     this.__emit(messageDetails);;
 }
 
 
 /**
  * Emit message to specific task.
  * @param messageDetails json with the emit details.
  *
  * For bolt, the json must contain the required fields:
  * - tuple - the value to emit
  * - anchorTupleId - the value of the anchor tuple (the input tuple that lead to this emit). Used to track the source
  * tuple and return ack when all components successfully finished to process it.
  * - task - indicate the task to send the tuple to.
  * and may contain the optional fields:
  * - stream (if empty - emit to default stream)
  *
  * For spout, the json must contain the required fields:
  * - tuple - the value to emit
  * - task - indicate the task to send the tuple to.
  * and may contain the optional fields:
  * - id - pass id for reliable emit (and receive ack/fail later).
  * - stream - if empty - emit to default stream.
  *
  * @param onTaskIds function than will be called with list of task ids the message was emitted to (when received).
  */
 Storm.prototype.emitDirect = function(commandDetails) {
     if (!commandDetails.task) {
         throw new Error("Emit direct must receive task id!")
     }
     this.__emit(commandDetails);
 }
 
 /**
  * Initialize storm component according to the configuration received.
  * @param conf configuration object accrding to storm protocol.
  * @param context context object according to storm protocol.
  * @param done callback. Call this method when finished initializing.
  */
 Storm.prototype.initialize = function(conf, context, done) {
     done();
 }
 
 Storm.prototype.run = function() {
     this.startReadingInput();
 }
 
 function Tuple(id, component, stream, task, values) {
     this.id = id;
     this.component = component;
     this.stream = stream;
     this.task = task;
     this.values = values;
 }
 
 /**
  * Base class for storm bolt.
  * To create a bolt implement 'process' method.
  * You may also implement initialize method to
  */
 function BasicBolt() {
     Storm.call(this);
     this.anchorTuple = null;
 };
 
 BasicBolt.prototype = Object.create(Storm.prototype);
 BasicBolt.prototype.constructor = BasicBolt;
 
 /**
  * Emit message.
  * @param commandDetails json with the required fields:
  * - tuple - the value to emit
  * - anchorTupleId - the value of the anchor tuple (the input tuple that lead to this emit). Used to track the source
  * tuple and return ack when all components successfully finished to process it.
  * and the optional fields:
  * - stream (if empty - emit to default stream)
  * - task (pass only to emit to specific task)
  */
 BasicBolt.prototype.__emit = function(commandDetails) {
     var self = this;
 
     var message = {
         command: "emit",
         tuple: commandDetails.tuple,
         stream: commandDetails.stream,
         task: commandDetails.task,
         anchors: [commandDetails.anchorTupleId]
     };
 
     this.sendMsgToParent(message);
 }
 
 BasicBolt.prototype.handleNewCommand = function(command) {
     var self = this;
     var tup = new Tuple(command["id"], command["comp"], command["stream"], command["task"], command["tuple"]);
     var callback = function(err) {
           if (err) {
               self.fail(tup, err);
           }
           self.ack(tup);
       }
     this.process(tup, callback);
 }
 
 /**
  * Implement this method when creating a bolt. This is the main method that provides the logic of the bolt (what
  * should it do?).
  * @param tuple the input of the bolt - what to process.
  * @param done call this method when done processing.
  */
 BasicBolt.prototype.process = function(tuple, done) {};
 
 BasicBolt.prototype.ack = function(tup) {
     this.sendMsgToParent({"command": "ack", "id": tup.id});
 }
 
 BasicBolt.prototype.fail = function(tup, err) {
     this.sendMsgToParent({"command": "fail", "id": tup.id});
 }
 
 
 /**
  * Base class for storm spout.
  * To create a spout implement the following methods: nextTuple, ack and fail (nextTuple - mandatory, ack and fail
  * can stay empty).
  * You may also implement initialize method.
  *
  */
 function Spout() {
     Storm.call(this);
 };
 
 Spout.prototype = Object.create(Storm.prototype);
 
 Spout.prototype.constructor = Spout;
 
 /**
  * This method will be called when an ack is received for preciously sent tuple. One may implement it.
  * @param id The id of the tuple.
  * @param done Call this method when finished and ready to receive more tuples.
  */
 Spout.prototype.ack = function(id, done) {};
 
 /**
  * This method will be called when an fail is received for preciously sent tuple. One may implement it (for example -
  * log the failure or send the tuple again).
  * @param id The id of the tuple.
  * @param done Call this method when finished and ready to receive more tuples.
  */
 Spout.prototype.fail = function(id, done) {};
 
 /**
  * Method the indicates its time to emit the next tuple.
  * @param done call this method when done sending the output.
  */
 Spout.prototype.nextTuple = function(done) {};
 
 Spout.prototype.handleNewCommand = function(command) {
     var self = this;
     var callback = function() {
         self.sync();
     }
 
     if (command["command"] === "next") {
         this.nextTuple(callback);
     }
 
     if (command["command"] === "ack") {
         this.ack(command["id"], callback);
     }
 
     if (command["command"] === "fail") {
         this.fail(command["id"], callback);
     }
 }
 
 /**
  * @param commandDetails json with the required fields:
  * - tuple - the value to emit.
  * and the optional fields:
  * - id - pass id for reliable emit (and receive ack/fail later).
  * - stream - if empty - emit to default stream.
  * - task - pass only to emit to specific task.
  */
 Spout.prototype.__emit = function(commandDetails) {
     var message = {
         command: "emit",
         tuple: commandDetails.tuple,
         id: commandDetails.id,
         stream: commandDetails.stream,
         task: commandDetails.task
     };
 
     this.sendMsgToParent(message);
 }
 
 module.exports.BasicBolt = BasicBolt;
 module.exports.Spout = Spout;
