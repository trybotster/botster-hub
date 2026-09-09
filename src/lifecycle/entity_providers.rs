//! Current provider registrations prepared and replaced on Host workers.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use botster_core::{PluginDescriptorKind, PluginOwnedDescriptor};

/// This record contains no protocol strings, payloads, or registration history.
#[derive(Clone, Debug)]
pub(crate) struct EntityProviderRegistration(Arc<AtomicBool>);

impl EntityProviderRegistration {
    pub(crate) fn is_live(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
}

#[derive(Clone, Default)]
pub(crate) struct EntityProviderRegistrations(
    Arc<Mutex<BTreeMap<String, BTreeMap<String, EntityProviderRegistration>>>>,
);

pub(super) struct PreparedEntityProviders {
    package: String,
    families: BTreeMap<String, EntityProviderRegistration>,
}

impl EntityProviderRegistrations {
    pub(super) fn prepare(
        &self,
        package: &str,
        descriptors: &[PluginOwnedDescriptor],
    ) -> PreparedEntityProviders {
        let families = descriptors
            .iter()
            .filter(|descriptor| descriptor.descriptor.kind == PluginDescriptorKind::EntityProvider)
            .map(|descriptor| descriptor.descriptor.descriptor_id.clone())
            .collect();
        self.prepare_families(package, families)
    }

    fn prepare_families(
        &self,
        package: &str,
        families: BTreeSet<String>,
    ) -> PreparedEntityProviders {
        let families = families
            .into_iter()
            .map(|family| {
                (
                    family,
                    EntityProviderRegistration(Arc::new(AtomicBool::new(false))),
                )
            })
            .collect();
        PreparedEntityProviders {
            package: package.to_string(),
            families,
        }
    }

    /// Call this after the old Core worker stops and before the new worker starts.
    pub(super) fn replace(&self, prepared: PreparedEntityProviders) {
        let mut index = self.0.lock().expect("entity provider registrations lock");
        if let Some(previous) = index.remove(&prepared.package) {
            for registration in previous.values() {
                registration.0.store(false, Ordering::Release);
            }
        }
        for registration in prepared.families.values() {
            registration.0.store(true, Ordering::Release);
        }
        index.insert(prepared.package, prepared.families);
    }

    pub(super) fn retire(&self, package: &str) {
        let mut index = self.0.lock().expect("entity provider registrations lock");
        if let Some(previous) = index.remove(package) {
            for registration in previous.values() {
                registration.0.store(false, Ordering::Release);
            }
        }
    }

    /// The calling Lua worker selects the exact package and family before enqueue.
    pub(crate) fn select(
        &self,
        package: &str,
        family: &str,
    ) -> Result<EntityProviderRegistration, String> {
        let index = self
            .0
            .lock()
            .map_err(|_| "entity provider registrations unavailable".to_string())?;
        index
            .get(package)
            .and_then(|families| families.get(family))
            .cloned()
            .ok_or_else(|| {
                format!("entity_publish family {family} is not provided by package {package}")
            })
    }

    #[cfg(test)]
    pub(crate) fn test_retire(&self, package: &str) {
        self.retire(package);
    }

    #[cfg(test)]
    pub(crate) fn test_register(&self, package: &str, family: &str) {
        let prepared = self.prepare_families(package, BTreeSet::from([family.to_string()]));
        self.replace(prepared);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provider_registrations_preserve_exact_ownership_and_retire_old_references() {
        let registrations = EntityProviderRegistrations::default();
        registrations.test_register("first", "first.item");
        let old = registrations.select("first", "first.item").unwrap();
        assert!(old.is_live());
        assert!(registrations.select("second", "first.item").is_err());
        let prepared =
            registrations.prepare_families("first", BTreeSet::from(["first.item".into()]));
        assert!(!prepared.families["first.item"].is_live());
        assert!(old.is_live());
        registrations.replace(prepared);
        let current = registrations.select("first", "first.item").unwrap();
        assert!(current.is_live());
        assert!(!old.is_live());
        assert!(!Arc::ptr_eq(&old.0, &current.0));
        registrations.retire("first");
        assert!(!current.is_live());
        assert!(registrations.select("first", "first.item").is_err());
        assert!(registrations.0.lock().unwrap().is_empty());
    }

    #[test]
    fn provider_registration_history_is_owned_only_by_retained_references() {
        let registrations = EntityProviderRegistrations::default();
        registrations.test_register("p", "p.item");
        let first = registrations.select("p", "p.item").unwrap();
        let first_weak = Arc::downgrade(&first.0);
        for _ in 0..512 {
            registrations.test_register("p", "p.item");
            let index = registrations.0.lock().unwrap();
            assert_eq!(index.len(), 1);
            assert_eq!(index["p"].len(), 1);
        }
        assert!(!first.is_live());
        assert!(first_weak.upgrade().is_some());
        drop(first);
        assert!(first_weak.upgrade().is_none());
    }

    #[test]
    fn provider_registration_selection_completes_after_index_release() {
        let registrations = EntityProviderRegistrations::default();
        registrations.test_register("p", "p.item");
        let guard = registrations.0.lock().unwrap();
        let worker_registrations = registrations.clone();
        let (started, ready) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            started.send(()).unwrap();
            worker_registrations.select("p", "p.item")
        });
        ready.recv().unwrap();
        drop(guard);
        assert!(worker.join().unwrap().unwrap().is_live());
    }
}
