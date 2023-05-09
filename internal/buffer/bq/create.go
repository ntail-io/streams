package bq

import (
	"context"
	"fmt"

	"cloud.google.com/go/bigquery"
	"github.com/google/uuid"
	"github.com/ntail-io/streams/internal/core/types"
)

const createTableDdl = "CREATE TABLE `%s.%s.%s` (" + `
  msg_id_time INTEGER NOT NULL,
  msg_id_clock_seq INTEGER NOT NULL,
  msg_id_node_id BYTES NOT NULL, -- 6 bytes
  msg_time TIMESTAMP NOT NULL,
  ` + "`hash`" + ` INTEGER NOT NULL,
  key STRING NOT NULL,
  data BYTES NOT NULL,

  ingestion_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP(), -- This might be redundant
)

CLUSTER BY msg_id_time, msg_id_clock_seq
;`

const createDatasetDdl = "CREATE SCHEMA IF NOT EXISTS `%s.%s` OPTIONS (default_table_expiration_days=7, location='%s')"

func createTable(ctx context.Context, client *bigquery.Client, projectId, dataset, table string) (err error) {
	q := fmt.Sprintf(createTableDdl, projectId, dataset, table)
	defer func() {
		if err != nil {
			err = fmt.Errorf("error creating table %s.%s.%s [%s]: %w", projectId, dataset, table, q, err)
		}
	}()
	job, err := client.Query(q).Run(ctx)
	if err != nil {
		return
	}
	_, err = job.Wait(ctx)
	return
}

func tableId(hr types.HashRange, from uuid.Time) string {
	return fmt.Sprintf("seg_%d_%d_%d", from, hr.From, hr.To)
}

func createDatasetIfNotExists(ctx context.Context, client *bigquery.Client, projectId, dataset string) (err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("error creating dataset %s.%s: %w", projectId, dataset, err)
		}
	}()

	if _, err := client.Dataset(dataset).Metadata(ctx); err == nil { // Check if already exists, this is to reduce risk of quota hits.
		return nil
	}

	q := fmt.Sprintf(createDatasetDdl, projectId, dataset, "europe-west1")
	job, err := client.Query(q).Run(ctx)
	if err != nil {
		return
	}
	_, err = job.Wait(ctx)
	return
}
