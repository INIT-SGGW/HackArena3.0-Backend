pub mod race {
    pub mod v1 {
        include!(concat!(env!("CARGO_MANIFEST_DIR"), "/gen/race.v1.rs"));
    }
}

pub mod weather {
    pub mod v1 {
        include!(concat!(env!("CARGO_MANIFEST_DIR"), "/gen/weather.v1.rs"));
    }
}

pub mod achievement {
    pub mod v1 {
        include!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/gen/achievement.v1.rs"
        ));
    }
}

pub mod hackarena {
    pub mod build {
        pub mod v1 {
            include!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/gen/hackarena.build.v1.rs"
            ));
        }
    }
}
