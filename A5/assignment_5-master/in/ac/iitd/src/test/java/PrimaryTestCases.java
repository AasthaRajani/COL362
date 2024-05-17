import convention.PConvention;
//import rules.PRules;

//import org.apache.calcite.tools.RuleSet;
//import org.apache.calcite.tools.RuleSets;
import org.apache.calcite.rel.RelNode;

import org.junit.Test;
import static org.junit.Assert.*;

import java.util.List;

public class PrimaryTestCases {


    public RelNode createRelNode(String query, MyCalciteConnection calciteConnection) {
        try{
            RelNode relNode = calciteConnection.convertSql(calciteConnection.validateSql(calciteConnection.parseSql(query)));
            return relNode;
        }
        catch(Exception e){
            throw new RuntimeException(e);
        }
    }

    public List<Object []> eval(RelNode relNode, MyCalciteConnection calciteConnection) {
        System.out.println("in eval 1");
        try{

            RelNode phyRelNode = calciteConnection.logicalToPhysical(
                relNode,
                relNode.getTraitSet().plus(PConvention.INSTANCE)
            );

            // You should check here that the physical relNode is a PProjectFilter instance
            System.out.println("in eval 2");
            System.out.println("[+] Physical RelNode:\n" + phyRelNode.explain());

            List<Object []> result = calciteConnection.executeQuery(phyRelNode);
            return result;
        }
        catch(Exception e){
            throw new RuntimeException(e);
        }
    }

    
    @Test 
    public void testSFW() {
        try{
            MyCalciteConnection calciteConnection = new MyCalciteConnection();
            String query = "select first_name from actor where actor_id > 100 and actor_id < 150";
//            String query = "select * from actor where actor_id > 100";
//            String query = "select last_name \n" +
//                    "from (select first_name, last_name from (select first_name, last_name, actor_id from actor where actor_id > 100) where actor_id < 150) \n" +
//                    "where first_name='Adam'";
//            String query = "select first_name from (select first_name, actor_id from actor where actor_id > 100) where actor_id > 100 and actor_id < 150";
//            String query = "select first_name from actor where true";
//            String query = "select 2 * actor_id, first_name from actor where 2.5 * (actor_id + 20.0) > 100";
//            String query = "select actor_id/2, first_name from actor where (actor_id/2) < 100";
//            String query = "select actor.first_name from actor left outer join new_actor on actor.first_name = new_actor.first_name";
//            String query = "select first_name, last_name from actor";
//            String query = "select title, description from film";
//            String query = "select film_id, title\n" +
//                    "from film\n" +
//                    "where release_year > 2005";
//            String query = "SELECT rental.rental_id, rental.rental_date, payment.amount\n" +
//                    "FROM rental\n" +
//                    "JOIN payment ON rental.rental_id = payment.rental_id";
//            String query = "select rental.rental_id, rental.rental_date, payment.amount\n" +
//                    "from rental, payment\n" +
//                    "where rental.rental_id = payment.rental_id";
//            String query = "select * from actor natural join film_actor";
//            String query = "SELECT customer.customer_id, customer.first_name, customer.last_name,\n" +
//                    "rental.rental_id, rental.rental_date\n" +
//                    "FROM customer\n" +
//                    "LEFT JOIN rental ON customer.customer_id = rental.customer_id";
//            String query = "select customer_id, sum(amount) from payment group by customer_id order by customer_id";
//            String query = "select store_id, count(customer_id) from customer group by store_id";
//            String query = "select film_id, count(film_id) \n" +
//                    "from rental r join inventory i on r.inventory_id = i.inventory_id \n" +
//                    "group by film_id \n" +
//                    "order by film_id";
//            String query = "select first_name, count(*) from actor group by first_name having count(*) > 1 order by first_name";
//            String query = "SELECT category.name, COUNT(category.category_id) AS film_count\n" +
//                    "FROM category\n" +
//                    "LEFT JOIN film_category ON category.category_id = film_category.category_id\n" +
//                    "GROUP BY category.name";
//            String query = "select customer_id, avg(amount) from payment group by customer_id order by customer_id";
//            String query = "select actor_id, sum(film_id) from film_actor group by actor_id order by actor_id";
//            String query = "select film.length from\n"
//                    + "film where rating = 'PG' and length<50";
//            String query = "select * from payment where amount>8*1.1";
            RelNode relNode = createRelNode(query, calciteConnection);
            System.out.println("[+] Logical RelNode:\n" + relNode.explain());
            List<Object []> result = eval(relNode, calciteConnection);
            System.out.println("Result size is "+result.size());
            assert(result.size() == 49);
            for(Object [] row : result){
                assert(row.length == 1);
            }
//             sort the result
            result.sort((a, b) -> ((String)a[0]).compareTo((String)b[0]));

            String [] expected = new String [] {
                "Adam",
                "Albert",
                "Albert",
                "Angela",
                "Cameron",
                "Cate",
                "Cate",
                "Cuba",
                "Dan",
                "Daryl",
                "Ed",
                "Emily",
                "Ewan",
                "Fay",
                "Frances",
                "Gene",
                "Gina",
                "Greta",
                "Groucho",
                "Harrison",
                "Jada",
                "Jane",
                "Julianne",
                "Kevin",
                "Kim",
                "Liza",
                "Lucille",
                "Matthew",
                "Morgan",
                "Morgan",
                "Morgan",
                "Penelope",
                "Penelope",
                "Renee",
                "Richard",
                "Rita",
                "River",
                "Russell",
                "Russell",
                "Salma",
                "Scarlett",
                "Sidney",
                "Susan",
                "Susan",
                "Sylvester",
                "Walter",
                "Warren",
                "Warren",
                "Whoopi"
            };

            for(int i = 0; i < result.size(); i++){
                assert(result.get(i)[0].equals(expected[i]));
            }

            // Tip: You can use the following code to print the result and debug

//             if(result == null) {
//                 System.out.println("[-] No result found");
//             }
//             else{
//                 System.out.println("[+] Final Output : ");
//                 for (Object [] row : result) {
//                     for (Object col : row) {
//                         System.out.print(col + " ");
//                     }
//                     System.out.println();
//                 }
//             }

            calciteConnection.close();
        }
        catch(Exception e){
            System.out.println(e);
            System.out.println(e.getCause());
            fail("Exception thrown");
        }
        System.out.println("Test passed :)");
        return;
    }

}