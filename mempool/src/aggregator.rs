use crate::config::{Committee, Stake};
use crate::error::{MempoolError, MempoolResult};
use crypto::{Digest, PublicKey, Signature};
use std::collections::HashSet;

pub struct Certificate {
    root: Digest,
    votes: Vec<(PublicKey, Signature)>,
}

pub struct Aggregator {
    weight: Stake,
    votes: Vec<(PublicKey, Signature)>,
    used: HashSet<PublicKey>,
}

impl Aggregator {
    pub fn new() -> Self {
        Self {
            weight: 0,
            votes: Vec::new(),
            used: HashSet::new(),
        }
    }

    /// Try to append a signature to a (partial) quorum.
    pub fn append(
        &mut self,
        root: Digest,
        signature: Signature,
        author: PublicKey,
        committee: &Committee,
    ) -> MempoolResult<Option<Certificate>> {
        // Ensure it is the first time this authority votes.
        ensure!(
            self.used.insert(author),
            MempoolError::AuthorityReuse(author)
        );

        self.votes.push((author, signature));
        //self.weight += committee.stake(&author);
        // TODO
        self.weight += 1;
        if self.weight >= committee.quorum_threshold() {
            self.weight = 0; // Ensures QC is only made once.
            return Ok(Some(Certificate {
                root,
                votes: self.votes.clone(),
            }));
        }
        Ok(None)
    }
}
