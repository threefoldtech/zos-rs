use std::collections::hash_map::Entry;
use std::collections::HashMap;

use std::fs;

pub struct Params(HashMap<String, Option<Vec<String>>>);

impl Params {
    pub fn exists<S: AsRef<str>>(&self, s: S) -> bool {
        self.0.get(s.as_ref()).is_some()
    }

    // values will return value assigned to flag for example "key=1 key=2" will return Some([1, 2])
    // if key exists but has no values, will return None. you can check if key exist with exists method
    pub fn values<S: AsRef<str>>(&self, k: S) -> Option<&Vec<String>> {
        match self.0.get(k.as_ref()) {
            None => None,
            Some(v) => match v {
                None => None,
                Some(v) => Some(v),
            },
        }
    }

    pub fn value<S: AsRef<str>>(&self, k: S) -> Option<&str> {
        match self.0.get(k.as_ref()) {
            None => None,
            Some(v) => match v {
                Some(v) if v.len() > 0 => Some(v[v.len() - 1].as_str()),
                _ => None,
            },
        }
    }
}

fn parse_params(content: String) -> Params {
    let mut params_map = HashMap::new();
    if let Some(cmdline) = shlex::split(&content) {
        for option in cmdline {
            let mut parts = option.splitn(2, "=").into_iter();
            // use this to make sure element exists
            let key = match parts.next() {
                Some(key) => key,
                None => continue,
            };

            match params_map.entry(key.to_string()) {
                Entry::Vacant(e) => {
                    match parts.next() {
                        Some(value) => e.insert(Some(vec![value.to_string()])),
                        None => e.insert(None),
                    };
                }
                Entry::Occupied(mut e) => match parts.next() {
                    Some(value) => match e.get_mut() {
                        Some(old_value) => old_value.push(value.to_string()),
                        None => continue,
                    },
                    None => continue,
                },
            }
        }
    }

    Params(params_map)
}

//params Get kernel cmdline arguments
pub fn get() -> Params {
    let content = match fs::read_to_string("/proc/cmdline") {
        Ok(content) => content,
        Err(err) => {
            log::error!("failed to get cmdline: {}", err);
            return Params(HashMap::default());
        }
    };

    parse_params(content)
}

#[cfg(test)]
mod test {
    use crate::kernel::parse_params;

    #[test]
    fn test_parse_params() {
        let content = "intel_iommu=on kvm-intel.nested=1 console=ttyS1,115200n8 console=\"tty1\" consoleblank=0 earlyprintk=serial,ttyS1,115200n8 with_spaces=\"with spaces\" loglevel=7 console=ttyS1,115200n8 zos-debug zos-debug-vm farmer_id=\"11\" runmode=dev version=v3 nomodeset";
        let params = parse_params(content.into());
        let console_values = params.values("console").unwrap();
        assert_eq!(console_values.len(), 3);
        assert_eq!(
            console_values,
            &vec![
                String::from("ttyS1,115200n8"),
                String::from("tty1"),
                String::from("ttyS1,115200n8")
            ]
        );
        assert_eq!(params.exists("zos-debug-vm"), true);
        assert_eq!(
            params.values("kvm-intel.nested").unwrap(),
            &vec![String::from("1")]
        );

        assert_eq!(params.value("farmer_id"), Some("11"));
        assert_eq!(params.value("with_spaces"), Some("with spaces"))
    }
}
