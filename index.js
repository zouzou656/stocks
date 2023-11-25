const express = require("express");
const http = require("http");
const socketIo = require("socket.io");
const { spawn } = require("child_process");
const cors = require("cors");
const kafka = require("kafka-node");
const app = express();
const server = http.createServer(app);
const fs = require("fs");
const { parse } = require("csv-parse");

const kafkaClient = new kafka.KafkaClient({ kafkaHost: "PLAINTEXT://localhost:9092" });
const producer = new kafka.Producer(kafkaClient);

producer.on("ready", function () {
  console.log("Kafka Producer is ready");
});

producer.on("error", function (err) {
  console.error("Error initializing Kafka Producer: " + err);
});

const readStream = fs.createReadStream("./data.csv");
const csvParser = parse({ delimiter: ",", from_line: 1 });

let isFirstRow = true;
let shouldClearTopic = false;

const io = socketIo(server, {
  cors: {
    origin: "*",
    methods: ["GET", "POST"],
  },
});

app.use((req, res, next) => {
  res.header("Access-Control-Allow-Origin", "*");
  res.header(
    "Access-Control-Allow-Headers",
    "Origin, X-Requested-With, Content-Type, Accept"
  );
  next();
});

app.use(express.static("public"));

io.on("connection", (socket) => {
  console.log("A user connected");

  io.emit("chat message", "A new user joined the chat");

  readStream.pipe(csvParser);
  let jsonData = [];
  csvParser.on("data", function (row) {
    if (isFirstRow) {
      columnNames = row;
      isFirstRow = false;
    } else {
      const jsonObject = {};
      row.map((value, index) => {
        jsonObject[columnNames[index]] = value;
      });
      jsonData.push(jsonObject);
    }
  });

  csvParser.on("end", function () {
    var i = 0;

    function sendToKafkaAndWebSocket() {
      setTimeout(function () {
        console.log(jsonData[i]);
  
        // Send to Kafka
        const payloads = [
          {
            topic: "Flink_Stocks",
            messages: JSON.stringify(jsonData[i]),
          },
        ];
  
        producer.send(payloads, function (err, data) {
          if (err) {
            console.error("Error sending data to Kafka: " + err);
          } else {
            console.log("Data sent to Kafka topic Flink_Stocks");
          }
        });
  
        // Send to WebSocket
        io.emit("newRow", jsonData[i]);
  
        i++;
        if (i < jsonData.length) {
          sendToKafkaAndWebSocket();
        } else {
          // All data sent, set shouldClearTopic to true
          shouldClearTopic = true;
        }
      }, 3000);
    }
  
    sendToKafkaAndWebSocket();
  });

  csvParser.on("error", function (error) {
    console.log(error.message);
  });

  socket.on("disconnect", () => {
    console.log("User disconnected");
    // When a user disconnects, set shouldClearTopic to true
    shouldClearTopic = true;
  });
});

// Handle process termination events (e.g., SIGINT, SIGTERM)
process.on("SIGINT", function () {
  // Send a special message to Kafka before exiting
  if (shouldClearTopic) {
    const clearTopicMessage = {
      topic: "Flink_Stocks",
      messages: "CLEAR_TOPIC_REQUEST",
    };

    producer.send([clearTopicMessage], function (err, data) {
      if (err) {
        console.error("Error sending clear topic request to Kafka: " + err);
      } else {
        console.log("Clear topic request sent to Kafka topic Flink_Stocks");
      }

      // Close the producer when done
      producer.close();
      process.exit();
    });
  } else {
    // If shouldClearTopic is false, just exit without sending a clear topic request
    producer.close();
    process.exit();
  }
});

const port = process.env.PORT || 3000;
server.listen(port, () => {
  console.log(`Server is listening on port ${port}`);
});
