use pgrx::prelude::*;
use pgrx::fcinfo::*;
use pgrx::spi::Spi;
use scryer_prolog::machine;
use scryer_prolog::machine::parsed_results::{
    QueryResolution, prolog_value_to_json_string
};
use std::time::{Duration, Instant};

pgrx::pg_module_magic!();

#[pg_extern(sql = "CREATE FUNCTION plprolog_call_handler() RETURNS language_handler LANGUAGE c AS 'MODULE_PATHNAME', '@FUNCTION_NAME@';")]
unsafe fn plprolog_call_handler(fcinfo: pg_sys::FunctionCallInfo) -> pg_sys::Datum {
    let str : Option<&str> = pg_getarg(fcinfo, 0);
    let mut machine = machine::Machine::new_lib();

    // We need procedure OID to get the actual source
    let oid : pg_sys::Oid = unsafe {
        fcinfo.as_ref().unwrap().flinfo.as_ref().expect("flinfo present").fn_oid
    };

    // Consult the code as module
    let d1 = Instant::now();
    let code = get_code(oid);
    pgrx::notice!("get code took: {:?}", d1.elapsed());
    let d2 = Instant::now();
    machine.load_module_string("plprolog_user", code.clone());
    pgrx::notice!("consultation took: {:?}", d2.elapsed());
    //format!("got code: {0}, arg: {1}", code, str.expect("argument")).into_datum().expect("result")
    // Then query handle(In,Out)
    let d3 = Instant::now();
    let output = machine.run_query(format!("handle({0}, Out).", str.expect("argument present")));
    pgrx::notice!("query took: {:?}", d3.elapsed());
    let d4 = Instant::now();
    let result : Option<pg_sys::Datum> =
        match output {
            Ok(QueryResolution::Matches(results)) => {
                // FIXME: turn bindings into actual table result
                let out = results[0].bindings.get("Out").expect("Expected output binding");
                let result_str = format!("got results: {0}", prolog_value_to_json_string(out.clone()).as_str());
                result_str.into_datum()
            },
            Ok(b) => format!("got truth: {0}", b.to_string()).into_datum(),
            Err(e) => format!("Got error: {0}", e).as_str().into_datum()
        };
    let final_result = result.expect("output conversion");
    pgrx::notice!("output conversion took: {:?}", d4.elapsed());
    final_result
}

fn get_code(oid: pg_sys::Oid) -> String {
    match Spi::get_one::<&str>(format!("SELECT prosrc FROM pg_proc WHERE oid={0}", oid.as_u32().to_string()).as_str()) {
        Ok(Some(code)) => code.to_string(),
        Ok(None) => panic!("Code for procedure not found"),
        Err(err) => panic!("SPI error: {0}", err)
    }
}
