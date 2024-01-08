package ma.ehei.sparkETL.service;

import ma.ehei.sparkETL.model.Employee;
import ma.ehei.sparkETL.repository.EmployeeRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class EmployeeService {
    @Autowired
    private EmployeeRepository employeeRepository;

    public List<Employee> getAllEmployees() {
        return employeeRepository.findAll();
    }

    public void saveEmployee(Employee employee) {
        employeeRepository.save(employee);
    }
    public void saveAllEmployees(List<Employee> employees) {
        employeeRepository.saveAll(employees);
    }
}
