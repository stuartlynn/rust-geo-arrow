// SO FAR HAVE MANGED TO READ IN THE PARQUET FILE AND INSPECT THE VARIOUS 
// SCHEMA.
// HAVE IDENTIFIED THE Geometry column and attempting to read the actual WKT 
// values to be able to decode.

use polars::prelude::*;
use polars::{df};
use arrow2::io::parquet::read;
use arrow2::datatypes::{PhysicalType};
use arrow2::array::{Array,BinaryArray};
use std::fs::File;
use polars::prelude::ParquetReader;
use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use wkb::*;


fn read_geom_column(file: &str){
    let mut f = File::open(file).expect("failed to open file");
    let meta = read::read_metadata(&mut f).expect("Failed to read metadata");
    let schema =  read::get_schema(&meta).expect("Failed to read schema");
    // let col_index = schema.fields().map(|f| f.)
    let col_pos  = schema.fields().iter().position(|f| f.name == String::from("geometry")).expect("Failed to find geom column");

    let geom_meta = meta.row_groups[0].column(col_pos);
    let pages = read::get_page_iterator(geom_meta, &mut f, None, vec![]).expect("Failed to create a page iterator");
    let data_type = schema.fields()[col_pos].data_type().clone();

    println!("Geom meta {:?}", geom_meta);
    println!("Geometry data type is {:?} ", data_type);

    let mut pages = read::Decompressor::new(pages, vec![]);
    let result = read::page_iter_to_array(&mut pages,geom_meta, data_type).unwrap();  

    let array_data_type = result.data_type();
    println!("Array data type {:?}", array_data_type);

    // This is downcasting to a None type.... not sure why
    let concrete_array = match result.data_type().to_physical_type(){
        PhysicalType::Binary => {
            let array = result.as_any().downcast_ref::<BinaryArray<i32>>().unwrap();
            let array = array.clone();
            Ok(Box::new(array))
        },
        _ =>Err("Failed to cast to concrete type")
    };

    let concrete_array = concrete_array.unwrap();

    for entry in concrete_array.iter(){
        let wkb = match entry{
            Some(wkb)=>{
                let mut bytes_cursor = Cursor::new(wkb);
                let p = bytes_cursor.read_wkb().unwrap();
                Some(p)
            },
            None => None 
        };
        println!("{:?}", wkb)
    }
    
    let array_cursor = Cursor::new(&result);
    // let geoms =  wkb::(array_cursor);


    for field in schema.fields(){
        println!("Field is {:?}",field);
    }

    println!("result is {:?} ", result.len());
    // println!("geoms are {:?}", geoms);

}

fn read_geom_records(file: &str){
    let reader = File::open(file).expect("failed to open file");
    let reader = read::RecordReader::try_new(reader, None, None, None, None).expect("failed to generate reader");
    for mabey_batch in reader{
        let batch = mabey_batch.expect("failed to get batch");
        let column = batch.column(30);
        println!("column, {:?}", column.len());
        println!("rows {}, {}", batch.num_rows(),batch.num_columns() );
    }
}


fn main() {
    let df = df![
        "a" => [1, 2, 3],
        "b" => [None, Some("a"), Some("b")]
    ].expect("unable to create dataframe");

        // read_geom_records("sample.parq");
    read_geom_column("sample.parq");


//     if let Ok(file) = File::open("sample.parq"){
//         let reader = ParquetReader::new(file);
//         let pdf = reader.finish().unwrap();
//         println!("Parquet dataframe {}",pdf);
//     }
        
    // println!("Hello, world! {}", df);
}
