use spatter::prelude::*;

fn main() -> Result<()> {
    let sc = SpatterContext::builder()
        .master("local[4]")
        .get_or_create()?;
    let sum = sc
        .parallelize(vec![1, 2, 3, 4, 56])
        .map(|x| x * 2)
        .reduce(|a, b| a + b)?;
    println!("The sum is: {sum}");
    Ok(())
}
