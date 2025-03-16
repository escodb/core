'use strict'

const { assert } = require('chai')

function testCrypto (impl) {
  describe('randomBytes()', () => {
    it('generates random buffers', () => {
      let key = impl.randomBytes(16)
      assert.instanceOf(key, Buffer)
      assert.equal(key.length, 16)
    })
  })

  describe('SHA-256', () => {
    it('computes digests', async () => {
      let data = Buffer.from('some data', 'utf8')
      let hash = await impl.sha256.digest(data)

      assert.equal(
        hash.toString('base64'),
        'EweZDmulyhRes16ZGCqb7EZTG8VN32VqYCx4D6AkDe4=')
    })
  })

  describe('HMAC-SHA-256', () => {
    it('generates keys', async () => {
      let key = await impl.hmacSha256.generateKey()
      assert.instanceOf(key, Buffer)
      assert.equal(key.length, 64)
    })

    it('signs a message', async () => {
      let key = Buffer.from('wtznHJpRyQ1731UMy7JZD6JVO3w/siiGb8s9wyVZSGK+U/BVR1DqqOIccCmkfsPRtHhgbbcNSr5wi6eaNWBzFQ==', 'base64')
      let data = Buffer.from('hello world', 'utf8')
      let sig = await impl.hmacSha256.sign(key, data)

      assert.equal(
        sig.toString('base64'),
        'yyfAwelMFCQzZ/+q/2aymE5bGH7H29urJMY3ui5Y9ig=')
    })

    it('verifies a signature', async () => {
      let key = Buffer.from('q2pUhwnqBYm3HAwQ1BfNqXJ3oyWur4E7zCn/iq1iqrKznZwBN9G0VbgcnU6u4Q6HcharFpamwCwvXsWpzQXYVQ==', 'base64')
      let data = Buffer.from('hello world', 'utf8')
      let sig = await impl.hmacSha256.sign(key, data)

      let verified = await impl.hmacSha256.verify(key, data, sig)
      assert(verified)

      sig = impl.randomBytes(32)
      verified = await impl.hmacSha256.verify(key, data, sig)
      assert(!verified)
    })
  })

  describe('AES-256-GCM', () => {
    it('generates a key', async () => {
      let key = await impl.aes256gcm.generateKey()
      assert.instanceOf(key, Buffer)
      assert.equal(key.length, 32)
    })

    it('generates an IV', async () => {
      let iv = await impl.aes256gcm.generateIv()
      assert.instanceOf(iv, Buffer)
      assert.equal(iv.length, 12)
    })

    it('encrypts a message', async () => {
      let key = Buffer.from('jam1+7s+qyvQfaBZtIfS35/KSlt3QWlyr7OjsT6rp8E=', 'base64')
      let iv = Buffer.from('SYY1si0hQeE1bYxf', 'base64')
      let msg = Buffer.from('the quick brown fox jumps over the lazy dog', 'utf8')

      let data = await impl.aes256gcm.encrypt(key, iv, msg)

      assert.equal(
        data.toString('base64'),
        'm3YH1wwyxhqJpslNHenpylaa4lxDPcJhvqRWjVB4EZGPtUgYJCES5ASaXki+06gXyZ5FCKieWLnP/lg=')
    })

    describe('decrypt()', () => {
      let key = Buffer.from('hSZO6x/ffuPhW1aNmeSUB5vBV/ocTDtlbGeODN26Ovw=', 'base64')
      let iv = Buffer.from('H+5XRhyLPi/+j+8M', 'base64')

      async function assertRejects (fn) {
        let error
        try {
          await fn()
        } catch (e) {
          error = e
        }
        assert(error)
      }

      it('decrypts a message', async () => {
        let msg = Buffer.from('1lkc6Nq7DdZ3FC0B2McL33rjkjl868X1oxPforQphnInQKL9irlz', 'base64')

        let data = await impl.aes256gcm.decrypt(key, iv, msg)
        assert.equal(data.toString('utf8'), 'very secret information')
      })

      it('fails to decrypt a modified IV', async () => {
        let msg = Buffer.from('Xlkc6Nq7DdZ3FC0B2McL33rjkjl868X1oxPforQphnInQKL9irlz', 'base64')
        await assertRejects(() => impl.aes256gcm.decrypt(key, iv, msg))
      })

      it('fails to decrypt a modified ciphertext', async () => {
        let msg = Buffer.from('1lkc6Nq7DdZ3FC0B2McLX3rjkjl868X1oxPforQphnInQKL9irlz', 'base64')
        await assertRejects(() => impl.aes256gcm.decrypt(key, iv, msg))
      })

      it('fails to decrypt a modified auth tag', async () => {
        let msg = Buffer.from('1lkc6Nq7DdZ3FC0B2McL33rjkjl868X1oxPforQphnInQKL9irlX', 'base64')
        await assertRejects(() => impl.aes256gcm.decrypt(key, iv, msg))
      })
    })
  })

  describe('PBKDF2', () => {
    it('generates a salt', async () => {
      let salt = await impl.pbkdf2.generateSalt()
      assert.instanceOf(salt, Buffer)
      assert.equal(salt.length, 32)
    })

    it('generates a key', async () => {
      let salt = Buffer.from('f6UORrixFECGBWvhqrkmSg==', 'base64')
      let key = await impl.pbkdf2.digest('open sesame', salt, 100, 128)

      assert.equal(key.toString('base64'), 'UnAUC8QzmfO/VQVA7x747Q==')
    })

    it('generates a longer key', async () => {
      let salt = Buffer.from('f6UORrixFECGBWvhqrkmSg==', 'base64')
      let key = await impl.pbkdf2.digest('open sesame', salt, 100, 256)

      assert.equal(
        key.toString('base64'),
        'UnAUC8QzmfO/VQVA7x747Rtd6r5NJMqlRADK0an+wYo=')
    })

    it('uses more iterations', async () => {
      let salt = Buffer.from('f6UORrixFECGBWvhqrkmSg==', 'base64')
      let key = await impl.pbkdf2.digest('open sesame', salt, 200, 256)

      assert.equal(
        key.toString('base64'),
        'JnM5x6YU0JfGuLE8LIpctscrvYRwv78etXN4oxNvXKo=')
    })

    it('returns the same result for NFC input', async () => {
      let pw = Buffer.from('6d61c3b1c3a16ec7a3', 'hex').toString('utf8')
      let salt = Buffer.from('GH+3OardBQexNBX4I0BJnw==', 'base64')

      let key = await impl.pbkdf2.digest(pw, salt, 100, 128)

      assert.equal(key.toString('base64'), 'VSlKlFXsn8KkDv0XhSdRwA==')
    })

    it('returns the same result for NFD input', async () => {
      let pw = Buffer.from('6d616ecc8361cc816ec3a6cc84', 'hex').toString('utf8')
      let salt = Buffer.from('GH+3OardBQexNBX4I0BJnw==', 'base64')

      let key = await impl.pbkdf2.digest(pw, salt, 100, 128)

      assert.equal(key.toString('base64'), 'VSlKlFXsn8KkDv0XhSdRwA==')
    })
  })
}

const NodeCrypto = require('../lib/crypto/node_crypto')
describe('crypto (node)', () => testCrypto(NodeCrypto))

const version = process.version.match(/\d+/g).map((n) => parseInt(n, 10))

if (version[0] >= 16) {
  const WebCrypto = require('../lib/crypto/web_crypto')
  describe('crypto (web)', () => testCrypto(WebCrypto))
}
