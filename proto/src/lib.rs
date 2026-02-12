#[cfg(any(proto_mode_local, feature = "proto-local"))]
pub mod race {
    pub mod v1 {
        include!(concat!(env!("CARGO_MANIFEST_DIR"), "/gen/race.v1.rs"));
    }
}

#[cfg(any(proto_mode_local, feature = "proto-local"))]
pub mod weather {
    pub mod v1 {
        include!(concat!(env!("CARGO_MANIFEST_DIR"), "/gen/weather.v1.rs"));
    }
}

#[cfg(any(proto_mode_published, not(feature = "proto-local")))]
pub mod race {
    pub mod v1 {
        compile_error!("Published proto not wired yet. Enable `--features proto-local`.");
    }
}

#[cfg(any(proto_mode_published, not(feature = "proto-local")))]
pub mod weather {
    pub mod v1 {
        compile_error!("Published proto not wired yet. Enable `--features proto-local`.");
    }
}
