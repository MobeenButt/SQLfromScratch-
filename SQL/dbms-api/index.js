const express = require('express');
const cors = require('cors');
const { spawn } = require('child_process');
const app = express();
const port = 3001;

// Persistent DBMS process with enhanced error handling
let dbmsProcess;
let commandQueue = [];
let isProcessing = false;
let isRestarting = false;

app.use(cors());
app.use(express.json());

// Error-tolerant DBMS process management
function startDBMS() {
  if (isRestarting) return;
  
  try {
    dbmsProcess = spawn(
      `${__dirname}/../SQL/bin/dbms.exe`,
      [],
      { 
        stdio: ['pipe', 'pipe', 'pipe'],
        windowsHide: true
      }
    );

    dbmsProcess.on('error', (err) => {
      console.error('DBMS Process Error:', err.message);
      scheduleRestart();
    });

    dbmsProcess.stderr.on('data', (data) => {
      const errorMsg = data.toString().trim();
      if (errorMsg) console.error('DBMS Runtime Error:', errorMsg);
    });

    dbmsProcess.on('exit', (code, signal) => {
      if (code !== 0) {
        console.error(`DBMS process exited unexpectedly with code ${code} and signal ${signal}`);
      }
      scheduleRestart();
    });

    isRestarting = false;
    console.log('DBMS process started successfully');

  } catch (err) {
    console.error('Failed to start DBMS process:', err.message);
    scheduleRestart();
  }
}

function scheduleRestart() {
  if (isRestarting) return;
  
  isRestarting = true;
  console.log('Attempting to restart DBMS process in 1 second...');
  setTimeout(() => {
    isRestarting = false;
    startDBMS();
  }, 1000);
}

// Robust command processing
async function processCommand(command, res) {
  return new Promise((resolve) => {
    if (!dbmsProcess || dbmsProcess.killed) {
      res.status(503).json({ 
        output: 'DBMS is temporarily unavailable. Please try again shortly.',
        error: true
      });
      return resolve();
    }

    let output = '';
    let timeoutId;
    const commandTimeout = 10000; // 10 seconds timeout

    const cleanup = () => {
      clearTimeout(timeoutId);
      dbmsProcess.stdout.off('data', dataHandler);
    };

    const dataHandler = (data) => {
      output += data.toString();
      
      // Reset timeout on any output
      clearTimeout(timeoutId);
      timeoutId = setTimeout(() => {
        cleanup();
        res.status(504).json({
          output: 'Command timed out',
          error: true
        });
        resolve();
      }, commandTimeout);

      // Detect command completion
      if (/(\n|\r\n)[\w]+> $/.test(output)) {
        cleanup();
        
        const cleanOutput = output
          .replace(/(\n|\r\n)[\w]+> $/, '')
          .replace(new RegExp(command), '')
          .trim();

        res.json({ 
          output: cleanOutput,
          error: cleanOutput.toLowerCase().includes('error')
        });
        resolve();
      }
    };

    timeoutId = setTimeout(() => {
      cleanup();
      res.status(504).json({
        output: 'Command timed out',
        error: true
      });
      resolve();
    }, commandTimeout);

    dbmsProcess.stdout.on('data', dataHandler);
    
    try {
      dbmsProcess.stdin.write(`${command}\n`);
    } catch (err) {
      cleanup();
      res.status(500).json({
        output: 'Failed to send command to DBMS',
        error: true
      });
      resolve();
    }
  });
}

// Queue management with error handling
app.post('/execute', async (req, res) => {
  try {
    const command = req.body.command;
    if (!command || typeof command !== 'string') {
      return res.status(400).json({
        output: 'Invalid command format',
        error: true
      });
    }

    commandQueue.push({ command, res });
    
    if (!isProcessing) {
      isProcessing = true;
      while (commandQueue.length > 0) {
        const current = commandQueue.shift();
        try {
          await processCommand(current.command, current.res);
        } catch (err) {
          console.error('Command processing error:', err);
          current.res.status(500).json({
            output: 'Internal server error during command processing',
            error: true
          });
        }
      }
      isProcessing = false;
    }
  } catch (err) {
    console.error('API endpoint error:', err);
    res.status(500).json({
      output: 'Internal server error',
      error: true
    });
  }
});

// Health check endpoint
app.get('/health', (req, res) => {
  res.json({
    status: dbmsProcess && !dbmsProcess.killed ? 'healthy' : 'unhealthy',
    queueLength: commandQueue.length
  });
});

// Start the server with error handling
const server = app.listen(port, () => {
  console.log(`API server running on http://localhost:${port}`);
  startDBMS();
});

// Handle server errors
server.on('error', (err) => {
  console.error('Server error:', err);
  process.exit(1);
});

// Graceful shutdown
process.on('SIGTERM', () => {
  console.log('Shutting down gracefully...');
  server.close(() => {
    if (dbmsProcess) dbmsProcess.kill();
    process.exit(0);
  });
});

process.on('SIGINT', () => {
  console.log('Received SIGINT. Shutting down...');
  server.close(() => {
    if (dbmsProcess) dbmsProcess.kill();
    process.exit(0);
  });
});