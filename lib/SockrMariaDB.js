const { SockrService } = require("miko-sockr")


class SockrMariaDB extends SockrService {
  

  constructor({pool, table, database, key, keys, identity=true} = {}) {
    super()
    if (!pool) throw new Error("Must provide a pool parameter.")
    if (!table) throw new Error("Must provide a table parameter.")
    if (keys) this.keys = keys 
    else if (key) this.keys = [key]
    else throw new Error("Must provide a key or keys parameter.")
    this.pool = pool
    this.table = table
    this.database = database || ""
    this.identity = identity
    if (this.database) 
      this.name = `\`${this.database}\`.\`${this.table}\``
    else
      this.name = `\`${this.table}\``    
  }


  parse(params, keys) {
    let fields = []
    let values = []
    let sets = []
    if (!keys) keys = Object.keys(params)
    for(let name of keys) {
      fields.push(`\`${name}\``)
      values.push(':'+name)
      sets.push(`\`${name}\`=:${name}`)
    }
    return { fields, values, sets }
  }

  

  async add(params)    {  
    if (this.identity) {
      for(let key of this.keys) {
        if (params[key]) throw new TypeError("The primary key cannot be in the parameters.")
      }
    }
    let parsed = this.parse(params)
    let query = { namedPlaceholders: true }
    query.sql = `INSERT INTO ${this.name} (${parsed.fields.join(', ')}) VALUES (${parsed.values.join(', ')}) `
    let result = await this.pool.query(query, params)
    return result
  }

  
  async query(sql, params) {
    let result = null
    if (params) {
      if (Array.isArray(params)) 
        result = await this.pool.query(sql, params)
      else
        result = await this.pool.query({ sql, namedPlaceholders: true }, params)
    } else {
      result = await this.pool.query(sql)
    }      
    if (result && result.meta) delete result.meta
    return result
  }


  async get(params, fields) {  
    for(let key of this.keys) {
      if (!params[key]) throw new TypeError("The primary key was not found in the parameters.")
    }
    let result = await this.find(params, fields)
    if (result && result.length > 0) return result[0]
    return result
  }


  async find(params, fields) {  
    let parsed = this.parse(params)
    let query = { namedPlaceholders: true }
    if (!fields) {
      query.sql = `SELECT * FROM ${this.name} WHERE ${parsed.sets.join(' AND ')} `
    } else if (Array.isArray(fields)) {
      for(let i=0; i < fields.length; i++) {
        if (!fields[i].includes("`"))
          fields[i] = `\`${fields[i]}\``
      }
      query.sql = `SELECT ${fields.join(", ")} FROM ${this.name} WHERE ${parsed.sets.join(' AND ')} `
    } else {
      throw new TypeError("The fields parameter must be an array or null.")
    }
    let result = await this.pool.query(query, params)
    if (result && result.meta) delete result.meta
    return result
  }


  async save(params)   {  
    for(let key of this.keys) {
      if (!params[key]) throw new TypeError("The primary key was not found in the parameters.")
    }
    let parsed = this.parse(params)
    let where = this.parse(params, this.keys)
    let query = { namedPlaceholders: true }
    query.sql = `UPDATE ${this.name} SET ${parsed.sets.join(', ')} WHERE ${where.sets.join(' AND ')} `    
    let result = await this.pool.query(query, params)
    return result
  }


  async remove(params) { 
    for(let key of this.keys) {
      if (!params[key]) throw new TypeError("The primary key was not found in the parameters.")
    }     
    let query = { namedPlaceholders: true }
    let where = this.parse(params, this.keys)
    query.sql = `DELETE FROM ${this.name} WHERE ${where.sets.join(' AND ')} `
    let result = await this.pool.query(query, params)
    return result
  }


  async upsert(params)   {  
    if (this.identity) {
      for(let key of this.keys) {
        if (params[key]) throw new TypeError("The primary key cannot be in the parameters.")
      }
    }
    let parsed = this.parse(params)
    let query = { namedPlaceholders: true }
    query.sql = `INSERT INTO ${this.name} (${parsed.fields.join(', ')}) VALUES (${parsed.values.join(', ')}) `
              + ` ON DUPLICATE KEY UPDATE ${parsed.sets.join(', ')}`
    let result = await this.pool.query(query, params)
    return result
  }  



  static create(options) {
    return new this(options)
  }


}

module.exports = SockrMariaDB
