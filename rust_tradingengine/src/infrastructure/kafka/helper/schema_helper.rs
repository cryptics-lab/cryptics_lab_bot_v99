use anyhow::{anyhow, Result};
use std::fs;
use std::path::{Path, PathBuf};
use log::{info, warn};

/// Helper for finding and managing Avro schemas
pub struct SchemaHelper {
    schema_dir: String,
}

impl SchemaHelper {
    pub fn new(schema_dir: String) -> Self {
        SchemaHelper { schema_dir }
    }
    
    /// Find the latest schema for a given schema type
    pub fn find_latest_schema(&self, schema_type: &str) -> Result<PathBuf> {
        let base_path = Path::new(&self.schema_dir);
        let schema_dir = base_path.join(schema_type);
        
        // If the directory doesn't exist, return error
        if !schema_dir.is_dir() {
            return Err(anyhow!("Schema directory not found: {:?}", schema_dir));
        }
        
        // Look for schema.json as the primary schema file
        let schema_json_path = schema_dir.join("schema.json");
        if schema_json_path.exists() {
            info!("Found schema.json for {}: {:?}", schema_type, schema_json_path);
            return Ok(schema_json_path);
        }
        
        // If schema.json doesn't exist, look for any .json file
        let mut latest_modified = None;
        let mut latest_path = None;
        
        let entries = match fs::read_dir(&schema_dir) {
            Ok(entries) => entries,
            Err(e) => {
                warn!("Failed to read schema directory {:?}: {}", schema_dir, e);
                return Err(anyhow!("Failed to read schema directory: {}", e));
            }
        };
        
        for entry in entries {
            if let Ok(entry) = entry {
                let path = entry.path();
                if path.is_file() {
                    if let Some(ext) = path.extension() {
                        if ext == "json" || ext == "avsc" {
                            // Get file metadata to find the most recently modified
                            if let Ok(metadata) = fs::metadata(&path) {
                                if let Ok(modified) = metadata.modified() {
                                    if latest_modified.is_none() || modified > latest_modified.unwrap() {
                                        latest_modified = Some(modified);
                                        latest_path = Some(path);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        
        if let Some(path) = latest_path {
            info!("Found latest schema at {:?}", path);
            return Ok(path);
        }
        
        Err(anyhow!("Could not find a schema file for {}", schema_type))
    }
    
    /// Read the schema content from a file
    pub fn read_schema_file(&self, path: &Path) -> Result<String> {
        match fs::read_to_string(path) {
            Ok(content) => Ok(content),
            Err(e) => Err(anyhow!("Failed to read schema file: {}", e)),
        }
    }
    
    /// Get the schema content for a given schema type
    pub fn get_schema_content(&self, schema_type: &str) -> Result<String> {
        let schema_path = self.find_latest_schema(schema_type)?;
        self.read_schema_file(&schema_path)
    }
}
