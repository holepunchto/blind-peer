const setupTestnet = require('hyperdht/testnet')
const { spawn } = require('child_process')

exports.setUpNetwork = async (t, size = 10, opts = {}) => {
  const testnet = await setupTestnet(size, opts)
  t.teardown(
    async () => {
      await testnet.destroy()
    },
    { order: Infinity }
  )
  return testnet
}

exports.execFileOnNetns = async (netns, file, args = [], opts = {}) => {
  return await new Promise((resolve, reject) => {
    const cp = spawn('ip', ['netns', 'exec', netns, process.execPath, file, ...args], {
      ...opts,
      stdio: ['ignore', 'pipe', 'pipe']
    })

    let stdout = ''
    let stderr = ''

    cp.stdout.on('data', (data) => {
      stdout += data.toString()
    })

    cp.stderr.on('data', (data) => {
      stderr += data.toString()
    })

    cp.on('error', reject)

    cp.on('close', (code) => {
      if (code === 0) {
        resolve(stdout)
        return
      }

      try {
        reject(JSON.parse(stderr))
      } catch {
        reject(new Error(`Command failed with code ${code}: ${stderr.trim()}`))
      }
    })
  })
}
