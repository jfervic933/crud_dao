/*
 * Interface que usa el patrón DAO sobre la tabla Persona
 */

package daw.carlos.modelo;

import java.sql.SQLException;
import java.util.List;

/**
 *
 * @author J. Carlos F. Vico <jcarlosvico@maralboran.es>
 */

public interface IPersona {
    
    // Método para obtener todos los registros de la tabla
    List<PersonaDTO> getAll() throws SQLException;
    
    // Méodo para obtener un registro a partir de la PK
    PersonaDTO findByPk(int pk) throws SQLException;
    
    // Método para insertar un registro
    int insertPersona (PersonaDTO persona) throws SQLException;
    
    // Método para insertar varios registros
    int insertPersona (List<PersonaDTO> lista) throws SQLException;
    
    // Método para borrar una persona
    int deletePersona (PersonaDTO p) throws SQLException;
    
    // Método para borrar toda la tabla
    int deletePersona() throws SQLException;
    
    // Método para modificar una persona. Se modifica a la persona que tenga esa 'pk'
    // con los nuevos datos que traiga la persona 'nuevosDatos'
    int updatePersona (int pk, PersonaDTO nuevosDatos) throws SQLException;
    
}
