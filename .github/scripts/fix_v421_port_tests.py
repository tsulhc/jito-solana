#!/usr/bin/env python3
from pathlib import Path

path = Path("validator/src/commands/run/args.rs")
text = path.read_text()

replacements = [
    (
        """                send_transaction_service_config: SendTransactionServiceConfig::default(),
                filter_keys: HashSet::new(),
            }
""",
        """                send_transaction_service_config: SendTransactionServiceConfig::default(),
                filter_keys: HashSet::new(),
                metrics_addr: None,
            }
""",
    ),
    (
        """                send_transaction_service_config: self.send_transaction_service_config.clone(),
                filter_keys: self.filter_keys.clone(),
            }
""",
        """                send_transaction_service_config: self.send_transaction_service_config.clone(),
                filter_keys: self.filter_keys.clone(),
                metrics_addr: self.metrics_addr,
            }
""",
    ),
]

for old, new in replacements:
    if new in text:
        continue
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"expected one test initializer anchor, found {count}")
    text = text.replace(old, new, 1)

path.write_text(text)
print("v4.2.1 RunArgs test plumbing fixed")
