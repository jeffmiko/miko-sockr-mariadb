const { SockrService } = require("miko-sockr")


class SockrMariaDB extends SockrService {
  

  constructor({pool, table, database, key} = {}) {
    super()
    this.pool = pool
    this.table = table
    this.database = database 
    this.key = key
    if (this.database) 
      this.name = `\`${this.database}\`.\`${this.table}\``
    else
      this.name = `\`${this.table}\``    
  }


  parse(params) {
    let fields = []
    let values = []
    let sets = []

    for(let name of Object.keys(params)) {
      fields.push(`\`${name}\``)
      values.push(':'+name)
      sets.push(`\`${name}\`=:${name}`)
    }
    return { fields, values, sets }
  }


  async add(params)    {  
    if (params[this.key]) throw new TypeError("The primary key cannot be in the parameters.")
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
    if (!params[this.key]) throw new TypeError("The primary key was not found in the parameters.")
    let query = { namedPlaceholders: true }
    if (!fields) {
      query.sql = `SELECT * FROM ${this.name} WHERE \`${this.key}\` = :${this.key} `
    } else if (Array.isArray(fields)) {
      for(let i=0; i < fields.length; i++) {
        if (!fields[i].includes("`"))
          fields[i] = `\`${fields[i]}\``
      }
      query.sql = `SELECT ${fields.join(", ")} FROM ${this.name} WHERE \`${this.key}\` = :${this.key} `
    } else {
      throw new TypeError("The fields parameter must be an array or null.")
    }
    let result = await this.pool.query(query, params)
    if (result && result.meta) delete result.meta
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
    if (!params[this.key]) throw new TypeError("The primary key was not found in the parameters.")
    let parsed = this.parse(params)
    let query = { namedPlaceholders: true }
    query.sql = `UPDATE ${this.name} SET ${parsed.sets.join(', ')} WHERE \`${this.key}\` = :${this.key} `    
    let result = await this.pool.query(query, params)
    return result
  }


  async remove(params) {  
    if (!params[this.key]) throw new TypeError("The primary key was not found in the parameters.")
    let query = { namedPlaceholders: true }
    query.sql = `DELETE FROM ${this.name} WHERE \`${this.key}\` = :${this.key} `
    let result = await this.pool.query(query, params)
    return result
  }


  async upsert(params)   {  
    if (params[this.key]) throw new TypeError("The primary key cannot be in the parameters.")
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
