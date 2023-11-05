package linkpostgre

const createLinkTableQuery = `
		CREATE TABLE IF NOT EXISTS links(
			id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			url text UNIQUE,
			retrieved_at TIMESTAMP 
		);
`

const createEdgeTableQuery = `
		CREATE TABLE IF NOT EXISTS edges(
			id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			src UUID NOT NULL REFERENCES links(id) ON DELETE CASCADE,
			dst UUID NOT NULL REFERENCES links(id) ON DELETE CASCADE,
			update_at TIMESTAMP,
			CONSTRAINT edge_links UNIQUE(src,dst)
		);
`

const lookupLinkQuery = `
	SELECT id, url, retrieved_at
	FROM links
	WHERE id = $1
`

const edgeRemoveStaleQuery = `
	DELETE FROM edges 
	WHERE src=$1 and update_at < $2
`

const edgeUpsertQuery = `
	INSERT INTO edges (src, dst, update_at) 
	VALUES ($1, $2, NOW())
	ON CONFLICT (src,dst) DO UPDATE SET update_at=NOW()
	RETURNING id,update_at
`

const linkUpsertQuery = `
	INSERT INTO links (url, retrieved_at) 
	VALUES ($1, $2)
	ON CONFLICT (url) DO UPDATE SET retrieved_at=GREATEST(links.retrieved_at, $2)
	RETURNING id,retrieved_at
`

const edgesIterationQuery = `
	SELECT id, src, dst, update_at 
	FROM edges 
	WHERE src >= $1 AND src < $2 AND update_at < $3
`

const linksIterationQuery = `
	SELECT id, url, retrieved_at 
	FROM links 
	WHERE id >= $1 AND id < $2 AND retrieved_at < $3
	`
