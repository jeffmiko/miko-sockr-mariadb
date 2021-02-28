const SockrMariaDB = require("../lib/SockrMariaDB")
const mariadb = require("mariadb")

const pool = mariadb.createPool({ host: 'mymoodle.local', user:'moodleroot', password: '' });
const database = "moodle"
const table = "mdl_user"
const key = "id"

const svc = new SockrMariaDB({pool, table, database, key})

svc.get({ id: 3}, ["id","username","firstname","lastname","email"]).then(data => console.log(data))
svc.find({ id: 3, lastname: "Miko"}, ["id","username","firstname","lastname","email"]).then(data => console.log(data))
svc.save({ id: 3, policyagreed: 1}).then(data => console.log(data))

setTimeout(() => { pool.end() }, 7000)
