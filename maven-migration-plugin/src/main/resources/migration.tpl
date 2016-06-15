<package>

import org.springframework.transaction.annotation.Transactional;
import com.knockchat.sql.migration.Migration;
import com.knockchat.sql.migration.AbstractMigration;

@Migration(module = "core", version = "<version>" )
public class <migrationName> extends AbstractMigration {

    @Override
    @Transactional
    public void up() throws Exception{
        //please insert your code here
    }

    @Override
    @Transactional
    public void down() throws Exception {
        //please insert your code here
    }
}