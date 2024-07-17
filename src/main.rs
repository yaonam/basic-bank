use csv::{Reader, ReaderBuilder, Trim, Writer};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::env;
use std::error::Error;
use std::fs::File;
use std::io;

#[derive(Debug, Deserialize, PartialEq)]
struct Bank {
    clients: HashMap<u16, Client>,          // client_id as key
    txs: HashMap<u32, (Transaction, bool)>, // tx_id -> (tx, disputed)
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
struct Client {
    client: u16, // redundant, for simple serializing
    available: f64,
    held: f64,
    total: f64,
    locked: bool,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
enum TransactionType {
    #[serde(rename = "deposit")]
    DEPOSIT,
    #[serde(rename = "withdrawal")]
    WITHDRAWAL,
    #[serde(rename = "dispute")]
    DISPUTE,
    #[serde(rename = "resolve")]
    RESOLVE,
    #[serde(rename = "chargeback")]
    CHARGEBACK,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq)]
struct Transaction {
    #[serde(rename = "type")]
    tx_type: TransactionType,
    #[serde(rename = "client")]
    client_id: u16,
    #[serde(rename = "tx")]
    tx_id: u32,
    #[serde(deserialize_with = "default_if_empty")]
    amount: f64,
}

fn default_if_empty<'de, D, T>(de: D) -> Result<T, D::Error>
where
    D: serde::Deserializer<'de>,
    T: serde::Deserialize<'de> + Default,
{
    use serde::Deserialize;
    Option::<T>::deserialize(de).map(|x| x.unwrap_or_else(|| T::default()))
}

impl Bank {
    pub fn new() -> Self {
        Bank {
            clients: HashMap::new(),
            txs: HashMap::new(),
        }
    }

    pub fn process_file(&mut self) -> Result<(), Box<dyn Error>> {
        // Get file paths
        let args: Vec<String> = env::args().collect();
        let input_path = &args[1];

        // Open file
        let rdr = ReaderBuilder::new().trim(Trim::All).from_path(input_path)?;

        // Process transactions
        self.process_txs(rdr)?;

        // Output file
        let mut wtr = Writer::from_writer(io::stdout());
        for (_, client) in self.clients.iter() {
            wtr.serialize(client)?;
            wtr.flush()?;
        }
        Ok(())
    }

    pub fn process_txs(&mut self, mut rdr: Reader<File>) -> Result<(), Box<dyn Error>> {
        // Iter txs, trim whitespaces
        for line in rdr.deserialize() {
            let tx: Transaction = line?;

            match tx.tx_type {
                TransactionType::DEPOSIT => {
                    self.deposit(tx);
                    self.txs.insert(tx.tx_id, (tx, false));
                }
                TransactionType::WITHDRAWAL => self.withdrawal(tx),
                TransactionType::DISPUTE => self.dispute(tx),
                TransactionType::RESOLVE => self.resolve(tx),
                TransactionType::CHARGEBACK => self.chargeback(tx),
            }
        }

        Ok(())
    }

    fn get_client(&self, client_id: u16) -> Option<&Client> {
        self.clients.get(&client_id)
    }

    fn set_client(&mut self, client: &Client) {
        self.clients.insert(client.client, *client);
    }

    fn get_tx(&self, tx_id: u32) -> Option<&(Transaction, bool)> {
        self.txs.get(&tx_id)
    }

    fn set_tx(&mut self, tx: &(Transaction, bool)) {
        self.txs.insert((*tx).0.tx_id, *tx);
    }

    fn deposit(&mut self, tx: Transaction) {
        if let Some(client) = self.get_client(tx.client_id) {
            // Update existing client
            self.set_client(&Client {
                client: client.client,
                available: client.available + tx.amount,
                held: client.held,
                total: client.total + tx.amount,
                locked: client.locked,
            });
        } else {
            // Add new client
            self.clients.insert(
                tx.client_id,
                Client {
                    client: tx.client_id,
                    available: tx.amount,
                    held: 0.0,
                    total: tx.amount,
                    locked: false,
                },
            );
        }
    }

    fn withdrawal(&mut self, tx: Transaction) {
        if let Some(client) = self.get_client(tx.client_id) {
            // Only update if sufficient balance
            if tx.amount <= client.available {
                self.set_client(&Client {
                    client: client.client,
                    available: client.available - tx.amount,
                    held: client.held,
                    total: client.total - tx.amount,
                    locked: client.locked,
                });
            }
        }
    }

    fn dispute(&mut self, dispute_tx: Transaction) {
        let tx: Transaction;
        let disputed: bool;
        {
            let tx_and_disputed = self.get_tx(dispute_tx.tx_id).clone();
            if let Some((_tx, _disputed)) = tx_and_disputed {
                tx = _tx.clone();
                disputed = *_disputed;
            } else {
                return;
            }
        }

        // Checks existence and already disputed
        if tx.client_id != dispute_tx.client_id
            || disputed
            || !self.clients.contains_key(&tx.client_id)
        {
            return;
        }

        if let Some(client) = self.get_client(tx.client_id) {
            // TODO handle tx.amount > client.available
            // TODO what if not deposit type?
            self.set_client(&Client {
                client: client.client,
                available: client.available - tx.amount,
                held: client.held + tx.amount,
                total: client.total,
                locked: client.locked,
            });
            self.set_tx(&(tx, true));
        }
    }

    fn resolve(&mut self, resolve_tx: Transaction) {
        let tx: Transaction;
        let disputed: bool;
        {
            let tx_and_disputed = self.get_tx(resolve_tx.tx_id).clone();
            if let Some((_tx, _disputed)) = tx_and_disputed {
                tx = _tx.clone();
                disputed = *_disputed;
            } else {
                return;
            }
        }

        if tx.client_id != resolve_tx.client_id
            || !disputed
            || !self.clients.contains_key(&tx.client_id)
        {
            return;
        }

        if let Some(client) = self.get_client(tx.client_id) {
            // TODO what if not deposit type?
            self.set_client(&Client {
                client: client.client,
                available: client.available + tx.amount,
                held: client.held - tx.amount,
                total: client.total,
                locked: client.locked,
            });
            self.set_tx(&(tx, false));
        }
    }

    fn chargeback(&mut self, chargeback_tx: Transaction) {
        let tx: Transaction;
        let disputed: bool;
        {
            let tx_and_disputed = self.get_tx(chargeback_tx.tx_id).clone();
            if let Some((_tx, _disputed)) = tx_and_disputed {
                tx = _tx.clone();
                disputed = *_disputed;
            } else {
                return;
            }
        }

        if tx.client_id != chargeback_tx.client_id
            || !disputed
            || !self.clients.contains_key(&tx.client_id)
        {
            return;
        }

        if let Some(client) = self.get_client(tx.client_id) {
            // TODO what if not deposit type?
            // TODO update disputed?
            self.set_client(&Client {
                client: client.client,
                available: client.available,
                held: client.held - tx.amount,
                total: client.total - tx.amount,
                locked: true,
            });
        }
    }
}

fn main() {
    let mut bank = Bank::new();
    bank.process_file().expect("Failed!");
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_and_run(path: &str) -> Result<Bank, Box<dyn Error>> {
        let rdr = ReaderBuilder::new().trim(Trim::All).from_path(path)?;
        let mut bank = Bank::new();
        bank.process_txs(rdr)?;

        Ok(bank)
    }

    #[test]
    fn deposit() -> Result<(), Box<dyn Error>> {
        let rdr = ReaderBuilder::new()
            .trim(Trim::All)
            .from_path("./tests/deposits.csv")?;
        let mut bank = Bank::new();
        bank.process_txs(rdr)?;

        assert_eq!(bank.clients[&1].available, 3.0);
        assert_eq!(bank.clients[&2].available, 2.0);

        Ok(())
    }

    #[test]
    fn withdrawal() -> Result<(), Box<dyn Error>> {
        let bank = create_and_run("./tests/withdrawals.csv")?;

        assert_eq!(bank.clients[&1].available, 1.5);
        assert_eq!(bank.clients[&2].available, 2.0);

        Ok(())
    }

    #[test]
    fn dispute() -> Result<(), Box<dyn Error>> {
        let bank = create_and_run("./tests/disputes.csv")?;

        assert_eq!(bank.clients[&1].available, 0.5);
        assert_eq!(bank.clients[&1].held, 1.0);
        assert_eq!(bank.clients[&1].total, 1.5);
        assert!(bank.txs[&1].1);

        assert_eq!(bank.clients[&2].available, 0.0);
        assert_eq!(bank.clients[&2].held, 2.0);
        assert_eq!(bank.clients[&2].total, 2.0);
        assert!(bank.txs[&2].1);

        Ok(())
    }

    #[test]
    fn resolve() -> Result<(), Box<dyn Error>> {
        let bank = create_and_run("./tests/resolves.csv")?;

        assert_eq!(bank.clients[&1].available, 1.5);
        assert_eq!(bank.clients[&1].held, 0.0);
        assert_eq!(bank.clients[&1].total, 1.5);
        assert!(!bank.txs[&1].1);

        assert_eq!(bank.clients[&2].available, 2.0);
        assert_eq!(bank.clients[&2].held, 0.0);
        assert_eq!(bank.clients[&2].total, 2.0);
        assert!(!bank.txs[&2].1);

        Ok(())
    }

    #[test]
    fn chargeback() -> Result<(), Box<dyn Error>> {
        let bank = create_and_run("./tests/chargebacks.csv")?;

        assert_eq!(bank.clients[&1].available, 0.5);
        assert_eq!(bank.clients[&1].held, 0.0);
        assert_eq!(bank.clients[&1].total, 0.5);
        assert!(bank.txs[&1].1);

        assert_eq!(bank.clients[&2].available, 0.0);
        assert_eq!(bank.clients[&2].held, 0.0);
        assert_eq!(bank.clients[&2].total, 0.0);
        assert!(bank.txs[&2].1);

        Ok(())
    }
}
