// Type-checked by `npm run typecheck`, never run.
import { LiveDb, ReadSetIndex, type CommitEvent, type QueryResult, type SqliveError } from 'sqlive'

interface Todo {
  id: number
  title: string
  done: number
}

async function example(): Promise<void> {
  const db = new LiveDb('app.db', { readers: 8, tableChanges: Infinity })
  const { value, version } = await db.write(async (tx) => {
    tx.exec('CREATE TABLE IF NOT EXISTS todos (id INTEGER PRIMARY KEY, list_id INTEGER, title TEXT, done INTEGER)')
    return tx.run('INSERT INTO todos (list_id, title) VALUES (?, ?)', [5, 'Buy milk']).lastInsertRowid
  })
  const id: number | bigint = value
  const sync = db.writeSync((tx) => tx.get<Todo>('SELECT * FROM todos WHERE id = :id', { id }))
  const todo: Todo | undefined = sync.value
  const first: QueryResult<Todo> = db.query<Todo>('SELECT id, title, done FROM todos WHERE list_id = ?', [5])
  const titles: string[] = first.rows.map((row) => row.title)
  const again = db.rerun('SELECT id, title, done FROM todos WHERE list_id = ?', [5], first.hash)
  const changed: boolean = !again.unchanged
  const index = new ReadSetIndex<number>()
  index.insert(1, first.readSet)
  for await (const event of db.changesSince(first.version)) {
    const keys: number[] = index.affected(event)
    const e: CommitEvent = event
    if (first.readSet.intersects(e) || keys.length > 0) break
  }
  for await (const result of db.subscribe<Todo>('SELECT * FROM todos ORDER BY id')) {
    const rows: Todo[] = result.rows
    if (rows.length > 0) break
  }
  try {
    await db.vacuum()
  } catch (error) {
    if ((error as SqliveError).code === 'ERR_SQLIVE_LAGGED') return
  }
  void [version, todo, titles, changed]
  db.close()
}

void example
