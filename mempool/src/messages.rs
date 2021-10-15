use crate::config::Committee;
use crate::error::{MempoolError, MempoolResult};
use crypto::{Digest, Hash, PublicKey, Signature, SignatureService};
use ed25519_dalek::Digest as _;
use ed25519_dalek::Sha512;
use merkle_light::hash::Algorithm;
use merkle_light::merkle::MerkleTree;
use merkle_light::proof::Proof;
use rust_crypto::digest::Digest as _;
use rust_crypto::sha3::{Sha3, Sha3Mode};
use serde::{Deserialize, Serialize};
use std::convert::TryInto;
use std::fmt;
use std::hash::Hasher;
use std::iter::FromIterator as _;

pub struct ExampleAlgorithm(Sha3);

impl ExampleAlgorithm {
    pub fn new() -> ExampleAlgorithm {
        ExampleAlgorithm(Sha3::new(Sha3Mode::Sha3_256))
    }
}

impl Default for ExampleAlgorithm {
    fn default() -> ExampleAlgorithm {
        ExampleAlgorithm::new()
    }
}

impl Hasher for ExampleAlgorithm {
    #[inline]
    fn write(&mut self, msg: &[u8]) {
        self.0.input(msg)
    }

    #[inline]
    fn finish(&self) -> u64 {
        unimplemented!()
    }
}

impl Algorithm<Digest> for ExampleAlgorithm {
    #[inline]
    fn hash(&mut self) -> Digest {
        let mut h = [0u8; 32];
        self.0.result(&mut h);
        Digest(h)
    }

    #[inline]
    fn reset(&mut self) {
        self.0.reset();
    }
}

#[derive(Debug)]
pub struct MerkleProof(pub Proof<Digest>);

#[derive(Debug)]
pub struct CodedPayload {
    pub chunk: Vec<u8>,
    pub proof: MerkleProof,
    pub author: PublicKey,
    pub signature: Signature,
}

impl CodedPayload {
    pub fn verify(&self, name: &PublicKey, committee: &Committee) -> MempoolResult<Digest> {
        let index = committee
            .index(name)
            .ok_or_else(|| MempoolError::BadInclusionProof)?;
        let mut hasher = Sha512::new();
        hasher.update(index.to_le_bytes());
        hasher.update(&self.chunk);
        let leaf = Digest(hasher.finalize().as_slice()[..32].try_into().unwrap());
        ensure!(self.proof.0.item() == leaf, MempoolError::BadInclusionProof);
        ensure!(
            self.proof.0.validate::<ExampleAlgorithm>(),
            MempoolError::BadInclusionProof
        );
        Ok(self.proof.0.root())
    }

    pub fn root(&self) -> Digest {
        self.proof.0.root()
    }
}

pub type Transaction = Vec<u8>;

#[derive(Deserialize, Serialize)]
pub struct Payload {
    pub transactions: Vec<Transaction>,
    pub author: PublicKey,
    pub signature: Signature,
}

impl Payload {
    pub async fn new(
        transactions: Vec<Transaction>,
        author: PublicKey,
        mut signature_service: SignatureService,
    ) -> Self {
        let payload = Self {
            transactions,
            author,
            signature: Signature::default(),
        };
        let signature = signature_service.request_signature(payload.digest()).await;
        Self {
            signature,
            ..payload
        }
    }

    pub fn size(&self) -> usize {
        self.transactions.iter().map(|x| x.len()).sum()
    }

    pub async fn encode_and_commit(
        &self,
        author: PublicKey,
        committee: &Committee,
        signature_service: &mut SignatureService,
    ) -> (Vec<CodedPayload>, Digest) {
        // Encode the payload using RS erasure codes. We can recover with f+1 chunks.
        let chunks = vec![vec![0u8, 1u8, 2u8]];

        // Computes the leaves of the Merkle Tree binding them to a node's index.
        let leaves: Vec<_> = chunks
            .iter()
            .enumerate()
            .map(|(i, x)| {
                let mut hasher = Sha512::new();
                hasher.update(i.to_le_bytes());
                hasher.update(&x);
                Digest(hasher.finalize().as_slice()[..32].try_into().unwrap())
            })
            .collect();

        // Commit to the encoded chunk.
        let tree: MerkleTree<Digest, ExampleAlgorithm> = MerkleTree::from_iter(leaves);
        let root = tree.root();
        let signature = signature_service.request_signature(root.clone()).await;
        let mut coded_payloads = Vec::new();
        for (i, chunk) in chunks.into_iter().enumerate() {
            let proof = tree.gen_proof(i);
            let coded_payload = CodedPayload {
                chunk,
                proof: MerkleProof(proof),
                author,
                signature: signature.clone(),
            };
            coded_payloads.push(coded_payload);
        }
        (coded_payloads, root)
    }
}

impl Hash for Payload {
    fn digest(&self) -> Digest {
        let mut hasher = Sha512::new();
        hasher.update(self.author.0);
        for transaction in &self.transactions {
            hasher.update(transaction);
        }
        Digest(hasher.finalize().as_slice()[..32].try_into().unwrap())
    }
}

impl fmt::Debug for Payload {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "Payload({}, {})", self.digest(), self.size())
    }
}
