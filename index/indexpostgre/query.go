package indexpostgre

// like matchDocQuery but whe $1 is blank or space it will return all
const searchMatchCountQuery = `
	SELECT COUNT(*) FROM documents
	WHERE ts @@ websearch_to_tsquery('english', $1)
`

// search words match
var searchMatchQuery = `
	SELECT linkID, url, title, content, indexed_at, pagerank
	FROM documents
	WHERE ts @@ websearch_to_tsquery('english', $1)
	ORDER BY
		ts_rank(ts, websearch_to_tsquery('english', $1)),
		pagerank DESC	

	OFFSET $2 ROWS
	LIMIT $3; --FETCH FIRST ($3) ROWS ONLY;
`

// search phrase match
var searchPhraseQuery = `
	SELECT linkID, url, title, content, indexed_at, pagerank
	FROM documents
	WHERE
		ts @@ phraseto_tsquery('english', $1)
	ORDER BY
		ts_rank(ts, phraseto_tsquery('english', $1)),	
		pagerank DESC

	OFFSET $2 ROWS
	LIMIT $3; --FETCH FIRST ($3) ROWS ONLY;
`

const searchPhraseCountQuery = `
	SELECT COUNT(*) FROM documents
	WHERE ts @@ phraseto_tsquery('english', $1)
`

// match anything (select all)
var searchAllQuery = `
	SELECT linkID, url, title, content, indexed_at, pagerank
	FROM documents
	ORDER BY linkID

	OFFSET $1 ROWS
	LIMIT $2; --FETCH FIRST ($2) ROWS ONLY;
`

const searchAllCountQuery = `
	SELECT COUNT(*) FROM documents
`

const updateScoreQuery = `
	UPDATE documents
	SET pagerank = $1 -- Replace with the pagerank value
	WHERE linkID = $2; -- Replace with the specific linkID 

`

const findDocumentQuery = `
	SELECT linkID, url, title, content, indexed_at, pagerank FROM documents
	WHERE linkID = $1
`

const insertDocumentQuery = `
	INSERT INTO documents (linkID, url, title, content, indexed_at, pagerank)
	VALUES($1,$2,$3,$4, $5, $6)
	ON CONFLICT (linkID) DO 
	UPDATE
		SET url = EXCLUDED.url,
			title = EXCLUDED.title,
			content = EXCLUDED.content,
			indexed_at = NOW();
`
