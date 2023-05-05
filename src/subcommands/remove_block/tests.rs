use std::slice;

use casper_node::types::{BlockHash, DeployHash, DeployMetadata};
use lmdb::{Error as LmdbError, Transaction, WriteFlags};

use crate::{
    common::db::{
        BlockBodyDatabase, BlockHeaderDatabase, Database, DeployMetadataDatabase, STORAGE_FILE_NAME,
    },
    subcommands::{
        execution_results_summary::block_body::BlockBody, remove_block::inner::remove_block,
    },
    test_utils::{
        mock_block_header, mock_deploy_hash, mock_deploy_metadata, LmdbTestFixture, MockBlockHeader,
    },
};

#[test]
fn try1() {
    const BLOCK_COUNT: usize = 2;
    const DEPLOY_COUNT: usize = 3;

    let test_fixture = LmdbTestFixture::new(
        vec![
            BlockHeaderDatabase::db_name(),
            BlockBodyDatabase::db_name(),
            DeployMetadataDatabase::db_name(),
        ],
        Some(STORAGE_FILE_NAME),
    );

    let deploy_hashes: Vec<DeployHash> = (0..DEPLOY_COUNT as u8).map(mock_deploy_hash).collect();
    let block_headers: Vec<(BlockHash, MockBlockHeader)> =
        (0..BLOCK_COUNT as u8).map(mock_block_header).collect();
    let mut block_bodies = vec![];
    let mut block_body_deploy_map: Vec<Vec<usize>> = vec![];
    block_bodies.push(BlockBody::new(vec![deploy_hashes[0], deploy_hashes[1]]));
    block_body_deploy_map.push(vec![0, 1]);
    block_bodies.push(BlockBody::new(vec![deploy_hashes[1], deploy_hashes[2]]));
    block_body_deploy_map.push(vec![1, 2]);

    let deploy_metadatas = vec![
        mock_deploy_metadata(slice::from_ref(&block_headers[0].0)),
        mock_deploy_metadata(&[block_headers[0].0, block_headers[1].0]),
        mock_deploy_metadata(slice::from_ref(&block_headers[1].0)),
    ];

    // Insert the 2 blocks into the database.
    {
        let mut txn = test_fixture.env.begin_rw_txn().unwrap();
        for i in 0..BLOCK_COUNT {
            // Store the header.
            txn.put(
                *test_fixture
                    .db(Some(BlockHeaderDatabase::db_name()))
                    .unwrap(),
                &block_headers[i].0,
                &bincode::serialize(&block_headers[i].1).unwrap(),
                WriteFlags::empty(),
            )
            .unwrap();
            // Store the body.
            txn.put(
                *test_fixture.db(Some(BlockBodyDatabase::db_name())).unwrap(),
                &block_headers[i].1.body_hash,
                &bincode::serialize(&block_bodies[i]).unwrap(),
                WriteFlags::empty(),
            )
            .unwrap();
        }

        // Insert the 3 deploys into the deploys and deploy_metadata databases.
        for i in 0..DEPLOY_COUNT {
            txn.put(
                *test_fixture
                    .db(Some(DeployMetadataDatabase::db_name()))
                    .unwrap(),
                &deploy_hashes[i],
                &bincode::serialize(&deploy_metadatas[i]).unwrap(),
                WriteFlags::empty(),
            )
            .unwrap();
        }
        txn.commit().unwrap();
    };

    assert!(remove_block(test_fixture.tmp_dir.path(), block_headers[0].0).is_ok());

    {
        let txn = test_fixture.env.begin_ro_txn().unwrap();
        assert_eq!(
            txn.get(
                *test_fixture
                    .db(Some(BlockHeaderDatabase::db_name()))
                    .unwrap(),
                &block_headers[0].0,
            )
            .unwrap_err(),
            LmdbError::NotFound
        );
        assert!(txn
            .get(
                *test_fixture
                    .db(Some(BlockHeaderDatabase::db_name()))
                    .unwrap(),
                &block_headers[1].0,
            )
            .is_ok());

        assert_eq!(
            txn.get(
                *test_fixture.db(Some(BlockBodyDatabase::db_name())).unwrap(),
                &block_headers[0].1.body_hash,
            )
            .unwrap_err(),
            LmdbError::NotFound
        );
        assert!(txn
            .get(
                *test_fixture.db(Some(BlockBodyDatabase::db_name())).unwrap(),
                &block_headers[1].1.body_hash,
            )
            .is_ok());

        assert_eq!(
            txn.get(
                *test_fixture
                    .db(Some(DeployMetadataDatabase::db_name()))
                    .unwrap(),
                &deploy_hashes[0]
            )
            .unwrap_err(),
            LmdbError::NotFound
        );

        let deploy_metadata: DeployMetadata = bincode::deserialize(
            txn.get(
                *test_fixture
                    .db(Some(DeployMetadataDatabase::db_name()))
                    .unwrap(),
                &deploy_hashes[1],
            )
            .unwrap(),
        )
        .unwrap();
        assert!(!deploy_metadata
            .execution_results
            .contains_key(&block_headers[0].0));
        assert!(deploy_metadata
            .execution_results
            .contains_key(&block_headers[1].0));

        let deploy_metadata: DeployMetadata = bincode::deserialize(
            txn.get(
                *test_fixture
                    .db(Some(DeployMetadataDatabase::db_name()))
                    .unwrap(),
                &deploy_hashes[2],
            )
            .unwrap(),
        )
        .unwrap();
        assert!(!deploy_metadata
            .execution_results
            .contains_key(&block_headers[0].0));
        assert!(deploy_metadata
            .execution_results
            .contains_key(&block_headers[1].0));
    }
}
