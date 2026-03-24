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

pub mod auth {
    pub mod v1 {
        include!(concat!(env!("CARGO_MANIFEST_DIR"), "/gen/auth.v1.rs"));
    }
}

pub mod hackarena {
    pub mod broker {
        pub mod v1 {
            include!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/gen/hackarena.broker.v1.rs"
            ));
        }
    }

    pub mod connect {
        pub mod v1 {
            include!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/gen/hackarena.connect.v1.rs"
            ));
        }
    }

    pub mod platform {
        pub mod common {
            pub mod v1 {
                include!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/gen/hackarena.platform.common.v1.rs"
                ));
            }
        }

        pub mod teams {
            pub mod v1 {
                include!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/gen/hackarena.platform.teams.v1.rs"
                ));
            }
        }
    }
}

pub mod submission {
    pub mod v1 {
        include!(concat!(env!("CARGO_MANIFEST_DIR"), "/gen/submission.v1.rs"));
    }
}
