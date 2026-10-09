const process = require('process')
const { spawn } = require('child_process')
const test = require('brittle')

if (process.argv[2] === 'child') process.exit(0)

test.solo('kill on a subprocess that already exited does not throw', async (t) => {
  const p = spawn(process.execPath, [__filename, 'child'])
  const exited = new Promise((resolve) => p.on('exit', resolve))

  const deadline = Date.now() + 2000
  let now = Date.now()
  while (now < deadline) now = Date.now()

  t.execution(() => p.kill(), 'kill does not throw')
  t.is(await exited, 0, 'child exited on its own before kill')
})
